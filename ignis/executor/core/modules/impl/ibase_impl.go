package impl

import (
	"ignis/executor/api"
	"ignis/executor/api/ipair"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
)

type IBaseImpl struct {
	executorData *core.IExecutorData
}

func (this *IBaseImpl) Context() api.IContext {
	return this.executorData.GetContext()
}

func Exchange[T any](this *IBaseImpl, in *storage.IPartitionGroup[T], out *storage.IPartitionGroup[T]) error {
	executors := this.executorData.Mpi().Executors()
	if executors == 1 {
		for _, part := range in.Iter() {
			if err := part.Fit(); err != nil {
				return ierror.Raise(err)
			}
			out.Add(part)
		}
		return nil
	}

	tp, err := this.executorData.GetProperties().ExchangeType()
	if err != nil {
		return ierror.Raise(err)
	}
	var sync bool
	if tp == "sync" {
		sync = true
	} else if tp == "async" {
		sync = false
	} else {
		logger.Info("Base: detecting exchange type")
		data := []impi.C_int64{impi.C_int64(in.Size()), 0}
		for _, part := range in.Iter() {
			if part.Empty() {
				data[1]++
			}
		}
		rank := this.executorData.Mpi().Rank()
		if err := impi.MPI_Reduce(utils.Ternary(rank == 0, impi.MPI_IN_PLACE, impi.P(&data[0])), impi.P(&data[0]), 2,
			impi.MPI_LONG, impi.MPI_SUM, 0, this.executorData.Mpi().Native()); err != nil {
			return ierror.Raise(err)
		}
		if this.executorData.Mpi().IsRoot(0) {
			n := int(data[0])
			nZero := int(data[1])
			sync = nZero < (n / executors)
		}
		aux := impi.C_int8(utils.Ternary(sync, 1, 0))
		if err := impi.MPI_Bcast(impi.P(&aux), 1, impi.MPI_BYTE, 0, this.executorData.Mpi().Native()); err != nil {
			return ierror.Raise(err)
		}
		sync = aux != 0
	}

	if sync {
		logger.Info("Base: using synchronous exchange")
		return exchangeSync[T](this, in, out)
	} else {
		logger.Info("Base: using asynchronous exchange")
		return exchangeAsync[T](this, in, out)
	}
}

func exchangeSync[T any](this *IBaseImpl, in *storage.IPartitionGroup[T], out *storage.IPartitionGroup[T]) error {
	executors := this.executorData.Mpi().Executors()
	numPartitions := in.Size()
	block := numPartitions / executors
	remainder := numPartitions % executors
	var partsTargets []ipair.IPair[int64, int64]

	none := *ipair.New(int64(-1), int64(-1))
	for i := 0; i < (block+1)*executors; i++ {
		partsTargets = append(partsTargets, none)
	}
	p := int64(0)
	for i := 0; i < executors; i++ {
		for j := 0; j < block; j++ {
			partsTargets[j*executors+i] = *ipair.New(p+int64(j), int64(i))
		}
		p += int64(block)
		if i < remainder {
			partsTargets[block*executors+i] = *ipair.New(p, int64(i))
			p += 1
		}
	}
	{
		var aux []ipair.IPair[int64, int64]
		for _, e := range partsTargets {
			if !ipair.Compare(&e, &none) {
				aux = append(aux, e)
			}
		}
		partsTargets = aux
	}

	for i := 0; i < block; i++ {
		for j := 0; j < executors; j++ {
			if j < remainder {
				partsTargets = append(partsTargets, *ipair.New(int64(block*j+i+j), int64(j)))
			} else {
				partsTargets = append(partsTargets, *ipair.New(int64(block*j+i+remainder), int64(j)))
			}
		}
	}

	if block > 0 {
		for j := 0; j < remainder; j++ {
			partsTargets = append(partsTargets, *ipair.New(int64(block*j+block), int64(j)))
		}
	} else {
		for j := 0; j < remainder; j++ {
			partsTargets = append(partsTargets, *ipair.New(int64(j), int64(j)))
		}
	}

	if err := this.executorData.EnableMpiCores(); err != nil {
		return ierror.Raise(err)
	}
	mpiCores := this.executorData.GetMpiCores()

	if err := ithreads.New().Static().Threads(mpiCores).Chunk(1).RunN(numPartitions, func(i int, sync ithreads.ISync) error {
		p := partsTargets[i].First
		target := partsTargets[i].Second

		if err := core.Gather(this.executorData.Mpi(), in.Get(int(p)), int(target)); err != nil {
			return ierror.Raise(err)
		}

		if this.executorData.Mpi().IsRoot(int(target)) {
			if err := in.Get(int(p)).Fit(); err != nil {
				return ierror.Raise(err)
			}
		} else {
			in.Set(int(p), nil)
		}
		return nil
	}); err != nil {
		return ierror.Raise(err)
	}

	for i := 0; i < numPartitions; i++ {
		if !in.Get(i).Empty() {
			out.Add(in.Get(i))
		}
	}
	in.Clear()
	return nil
}

func exchangeAsync[T any](this *IBaseImpl, in *storage.IPartitionGroup[T], out *storage.IPartitionGroup[T]) error {
	executors := this.executorData.Mpi().Executors()
	rank := this.executorData.Mpi().Rank()
	numPartitions := in.Size()
	block := numPartitions / executors
	remainder := numPartitions % executors
	var ranges []ipair.IPair[int64, int64]
	var queue []int64

	var init, end int64
	for i := 0; i < executors; i++ {
		if i < remainder {
			init = int64((block + 1) * i)
			end = init + int64(block+1)
		} else {
			init = int64((block+1)*remainder + block*(i-remainder))
			end = init + int64(block)
		}
		ranges = append(ranges, *ipair.New(init, end))
	}

	m := utils.Ternary(executors%2 == 0, executors, executors+1)
	id := 0
	id2 := m*m - 2
	for i := 0; i < executors; i++ {
		if rank == id%(m-1) {
			queue = append(queue, int64(m-1))
		}
		if rank == m-1 {
			queue = append(queue, int64(id%(m-1)))
		}
		id += 1
		for j := 1; j < m/2; j++ {
			if rank == id%(m-1) {
				queue = append(queue, int64(id2%(m-1)))
			}
			if rank == id2%(m-1) {
				queue = append(queue, int64(id%(m-1)))
			}
			id += 1
			id2 -= 1
		}
	}

	if err := this.executorData.EnableMpiCores(); err != nil {
		return ierror.Raise(err)
	}
	mpiCores := this.executorData.GetMpiCores()

	ignores := make([]bool, len(queue))

	if err := ithreads.New().Static().Threads(mpiCores).RunN(len(queue), func(i int, sync ithreads.ISync) error {
		other := queue[i]
		ignore := impi.C_int8(1)
		ignoreOther := impi.C_int8(1)
		if other == int64(executors) {
			return nil
		}
		for j := ranges[other].First; j < ranges[other].Second; j++ {
			ignore = utils.Ternary[impi.C_int8](ignore != 0 && in.Get(int(j)).Empty(), 1, 0)
		}
		if err := impi.MPI_Sendrecv(impi.P(&ignore), 1, impi.MPI_C_BOOL, impi.C_int(other), 0, impi.P(&ignoreOther), 1,
			impi.MPI_C_BOOL, impi.C_int(other), 0, this.executorData.Mpi().Native(), impi.MPI_STATUS_IGNORE); err != nil {
			return ierror.Raise(err)
		}

		if ignore != 0 && ignoreOther != 0 {
			ignores[i] = true
			for j := ranges[other].First; j < ranges[other].Second; j++ {
				in.Set(int(j), nil)
			}
		}
		return nil
	}); err != nil {
		return ierror.Raise(err)
	}

	for i := 0; i < len(queue); i++ {
		other := queue[i]
		if ignores[i] || other == int64(executors) {
			continue
		}
		otherPart := ranges[other].First
		otherEnd := ranges[other].Second
		mePart := ranges[rank].First
		meEnd := ranges[rank].Second
		its := int(utils.Max(otherEnd-otherPart, meEnd-mePart))

		if err := ithreads.New().Static().Threads(mpiCores).Chunk(1).RunN(its, func(j int, sync ithreads.ISync) error {
			mepart := ranges[rank].First + int64(j)
			otherPart := ranges[other].First + int64(j)
			if otherPart >= otherEnd || mepart >= meEnd {
				if otherPart >= otherEnd {
					if err := core.Recv(this.executorData.Mpi(), in.Get(int(mepart)), int(other), 0); err != nil {
						return ierror.Raise(err)
					}
				} else if mepart >= meEnd {
					if err := core.Send(this.executorData.Mpi(), in.Get(int(otherPart)), int(other), 0); err != nil {
						return ierror.Raise(err)
					}
				} else {
					return nil
				}
			} else {
				if err := core.SendRcv(this.executorData.Mpi(), in.Get(int(otherPart)), in.Get(int(mepart)), int(other), 0); err != nil {
					return ierror.Raise(err)
				}
			}
			in.Set(int(otherPart), nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
	}

	for i := 0; i < numPartitions; i++ {
		if !in.Get(i).Empty() {
			out.Add(in.Get(i))
		}
	}
	in.Clear()

	return nil
}
