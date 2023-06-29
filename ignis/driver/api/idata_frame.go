package api

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/driver/core"
	"ignis/executor/api/ipair"
	"ignis/executor/core/iio"
	"ignis/rpc/driver"
)

const (
	NO_CACHE   int8 = 0
	PRESERVE   int8 = 1
	MEMORY     int8 = 2
	RAW_MEMORY int8 = 3
	DISK       int8 = 4
        CHECKPOINT int8 = 5
)

type IDataFrame[T any] struct {
	worker *IWorker
	id     *driver.IDataFrameId
}

func newIDataFrame[T any](worker *IWorker, id *driver.IDataFrameId) *IDataFrame[T] {
	return &IDataFrame[T]{worker, id}
}

func (this *IDataFrame[T]) SetName(name string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SetName(context.Background(), this.id, name)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IDataFrame[T]) Persist(cacheLevel int8) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().Persist(context.Background(), this.id, cacheLevel)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IDataFrame[T]) Cache() error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().Cache(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IDataFrame[T]) Unpersist() error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().Unpersist(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IDataFrame[T]) Uncache() error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().Unpersist(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IDataFrame[T]) Repartition(numPartitions int64, preserveOrdering bool, global bool) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Repartition(context.Background(), this.id, numPartitions, preserveOrdering, global)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Repartition[T any](this *IDataFrame[T], numPartitions int64, preserveOrdering bool, global bool) (*IDataFrame[T], error) {
	return this.Repartition(numPartitions, preserveOrdering, global)
}

func (this *IDataFrame[T]) PartitionByRandom(numPartitions int64, seed int) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().PartitionByRandom(context.Background(), this.id, numPartitions, int32(seed))
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func PartitionByRandom[T any](this *IDataFrame[T], numPartitions int64, seed int) (*IDataFrame[T], error) {
	return this.PartitionByRandom(numPartitions, seed)
}

func (this *IDataFrame[T]) PartitionByHash(numPartitions int64) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().PartitionByHash(context.Background(), this.id, numPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func PartitionByHash[T any](this *IDataFrame[T], numPartitions int64) (*IDataFrame[T], error) {
	return this.PartitionByHash(numPartitions)
}

func (this *IDataFrame[T]) PartitionBy(src *ISource, numPartitions int64) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().PartitionBy(context.Background(), this.id, src.rpc(), numPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func PartitionBy[T any](this *IDataFrame[T], src *ISource, numPartitions int64) (*IDataFrame[T], error) {
	return this.PartitionBy(src, numPartitions)
}

func (this *IDataFrame[T]) Partitions() (int64, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return -1, err
	}
	defer client.Free()
	n, err := client.Services().GetDataframeService().Partitions(context.Background(), this.id)
	if err != nil {
		return -1, derror.NewGenericIDriverError(err)
	}
	return n, nil
}

func Partitions[T any](this *IDataFrame[T]) (int64, error) {
	return this.Partitions()
}

func (this *IDataFrame[T]) SaveAsObjectFile(path string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SaveAsObjectFile(context.Background(), this.id, path, 6)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func SaveAsObjectFile[T any](this *IDataFrame[T], path string) error {
	return this.SaveAsObjectFile(path)
}

func (this *IDataFrame[T]) SaveAsObjectFileComp(path string, compression int8) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SaveAsObjectFile(context.Background(), this.id, path, compression)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func SaveAsObjectFileComp[T any](this *IDataFrame[T], path string, compression int8) error {
	return this.SaveAsObjectFileComp(path, compression)
}

func (this *IDataFrame[T]) SaveAsTextFile(path string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SaveAsTextFile(context.Background(), this.id, path)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func SaveAsTextFile[T any](this *IDataFrame[T], path string) error {
	return this.SaveAsTextFile(path)
}

func (this *IDataFrame[T]) SaveAsJsonFile(path string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SaveAsJsonFile(context.Background(), this.id, path, true)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func SaveAsJsonFile[T any](this *IDataFrame[T], path string) error {
	return this.SaveAsJsonFile(path)
}

func (this *IDataFrame[T]) SaveAsJsonFilePretty(path string, pretty bool) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().SaveAsJsonFile(context.Background(), this.id, path, pretty)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func SaveAsJsonFilePretty[T any](this *IDataFrame[T], path string, pretty bool) error {
	return this.SaveAsJsonFilePretty(path, pretty)
}

func Map[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Map_(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame[T]) Filter(src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Filter(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Filter[T any](this *IDataFrame[T], src *ISource) (*IDataFrame[T], error) {
	return this.Filter(src)
}

func Flatmap[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Flatmap(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func KeyBy[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().KeyBy(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func MapWithIndex[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapWithIndex(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func MapPartitions[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapPartitions(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func MapPartitionsWithIndex[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapPartitionsWithIndex(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[R](this.worker, id), nil
}

func (this *IDataFrame[T]) MapExecutor(src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapExecutor(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func MapExecutor[T any](this *IDataFrame[T], src *ISource) (*IDataFrame[T], error) {
	return this.MapExecutor(src)
}

func MapExecutorTo[T any, R any](this *IDataFrame[T], src *ISource) (*IDataFrame[R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapExecutorTo(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[R]{
		this.worker,
		id,
	}, nil
}

func GroupBy[Key any, T any](this *IDataFrame[T], src *ISource) (*IPairDataFrame[Key, []T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().GroupBy(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, []T](this.worker, id), nil
}

func GroupByN[Key any, T any](this *IDataFrame[T], src *ISource, numPartitions int64) (*IPairDataFrame[Key, []T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().GroupBy2(context.Background(), this.id, src.rpc(), numPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, []T](this.worker, id), nil
}

func (this *IDataFrame[T]) Sort(ascending bool) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Sort(context.Background(), this.id, ascending)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Sort[T any](this *IDataFrame[T], ascending bool) (*IDataFrame[T], error) {
	return this.Sort(ascending)
}

func (this *IDataFrame[T]) SortN(ascending bool, numPartitions int64) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Sort2(context.Background(), this.id, ascending, numPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func SortN[T any](this *IDataFrame[T], ascending bool, numPartitions int64) (*IDataFrame[T], error) {
	return this.SortN(ascending, numPartitions)
}

func (this *IDataFrame[T]) SortBy(src *ISource, ascending bool) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().SortBy(context.Background(), this.id, src.rpc(), ascending)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func SortBy[T any](this *IDataFrame[T], src *ISource, ascending bool) (*IDataFrame[T], error) {
	return this.SortBy(src, ascending)
}

func (this *IDataFrame[T]) SortByN(src *ISource, ascending bool, numPartitions int64) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().SortBy3(context.Background(), this.id, src.rpc(), ascending, numPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func SortByN[T any](this *IDataFrame[T], src *ISource, ascending bool, numPartitions int64) (*IDataFrame[T], error) {
	return this.SortByN(src, ascending, numPartitions)
}

func (this *IDataFrame[T]) Union(other *IDataFrame[T], preserveOrder bool, src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().Union_(context.Background(), this.id, other.id, preserveOrder)
	} else {
		id, err = client.Services().GetDataframeService().Union4(context.Background(), this.id, other.id, preserveOrder, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Union[T any](this *IDataFrame[T], other *IDataFrame[T], preserveOrder bool, src *ISource) (*IDataFrame[T], error) {
	return this.Union(other, preserveOrder, src)
}

func (this *IDataFrame[T]) Distinct(src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().Distinct(context.Background(), this.id)
	} else {
		id, err = client.Services().GetDataframeService().Distinct2b(context.Background(), this.id, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Distinct[T any](this *IDataFrame[T], src *ISource) (*IDataFrame[T], error) {
	return this.Distinct(src)
}

func (this *IDataFrame[T]) DistinctN(numPartitions int64, src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().Distinct2a(context.Background(), this.id, numPartitions)
	} else {
		id, err = client.Services().GetDataframeService().Distinct3(context.Background(), this.id, numPartitions, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func DistinctN[T any](this *IDataFrame[T], numPartitions int64, src *ISource) (*IDataFrame[T], error) {
	return this.DistinctN(numPartitions, src)
}

/*General Action*/

func (this *IDataFrame[T]) Reduce(src *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Reduce(context.Background(), this.id, src.rpc(), tp)
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func Reduce[T any](this *IDataFrame[T], src *ISource) (T, error) {
	return this.Reduce(src)
}

func (this *IDataFrame[T]) TreeReduce(src *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().TreeReduce(context.Background(), this.id, src.rpc(), tp)
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func TreeReduce[T any](this *IDataFrame[T], src *ISource) (T, error) {
	return this.TreeReduce(src)
}

func (this *IDataFrame[T]) Collect() ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Collect(context.Background(), this.id, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func Collect[T any](this *IDataFrame[T]) ([]T, error) {
	return this.Collect()
}

func Aggregate[T any, R any](this *IDataFrame[T], zero, seqOp, combOp *ISource) (R, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(R), err
	}
	defer client.Free()
	tp := core.RegisterType[R](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Aggregate(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), tp)
	if err != nil {
		return *new(R), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[R](Ignis.callback.DriverContext(), id)
}

func TreeAggregate[T any, R any](this *IDataFrame[T], zero, seqOp, combOp *ISource) (R, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(R), err
	}
	defer client.Free()
	tp := core.RegisterType[R](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().TreeAggregate(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), tp)
	if err != nil {
		return *new(R), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[R](Ignis.callback.DriverContext(), id)
}

func (this *IDataFrame[T]) Fold(zero, src *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Fold(context.Background(), this.id, zero.rpc(), src.rpc(), tp)
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func Fold[T any](this *IDataFrame[T], zero, src *ISource) (T, error) {
	return this.Fold(zero, src)
}

func (this *IDataFrame[T]) TreeFold(zero, src *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().TreeFold(context.Background(), this.id, zero.rpc(), src.rpc(), tp)
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func TreeFold[T any](this *IDataFrame[T], zero, src *ISource) (T, error) {
	return this.TreeFold(zero, src)
}

func (this *IDataFrame[T]) Take(num int64) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Take(context.Background(), this.id, num, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func Take[T any](this *IDataFrame[T], num int64) ([]T, error) {
	return this.Take(num)
}

func (this *IDataFrame[T]) Foreach(src *ISource) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().Foreach_(context.Background(), this.id, src.rpc())
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func Foreach[T any](this *IDataFrame[T], src *ISource) error {
	return this.Foreach(src)
}

func (this *IDataFrame[T]) ForeachPartition(src *ISource) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().ForeachPartition(context.Background(), this.id, src.rpc())
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func ForeachPartition[T any](this *IDataFrame[T], src *ISource) error {
	return this.ForeachPartition(src)
}

func (this *IDataFrame[T]) ForeachExecutor(src *ISource) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetDataframeService().ForeachExecutor(context.Background(), this.id, src.rpc())
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func ForeachExecutor[T any](this *IDataFrame[T], src *ISource) error {
	return this.ForeachExecutor(src)
}

func (this *IDataFrame[T]) Top(num int64) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Top(context.Background(), this.id, num, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func Top[T any](this *IDataFrame[T], num int64) ([]T, error) {
	return this.Top(num)
}

func (this *IDataFrame[T]) TopCmp(num int64, cmp *ISource) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Top4(context.Background(), this.id, num, cmp.rpc(), tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func TopCmp[T any](this *IDataFrame[T], num int64, cmp *ISource) ([]T, error) {
	return this.TopCmp(num, cmp)
}

func (this *IDataFrame[T]) TakeOrdered(num int64) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().TakeOrdered(context.Background(), this.id, num, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func TakeOrdered[T any](this *IDataFrame[T], num int64) ([]T, error) {
	return this.TakeOrdered(num)
}

func (this *IDataFrame[T]) TakeOrderedCmp(num int64, cmp *ISource) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().TakeOrdered4(context.Background(), this.id, num, cmp.rpc(), tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func TakeOrderedCmp[T any](this *IDataFrame[T], num int64, cmp *ISource) ([]T, error) {
	return this.TakeOrderedCmp(num, cmp)
}

func (this *IDataFrame[T]) Sample(withReplacement bool, fraction float64, seed int) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().Sample(context.Background(), this.id, withReplacement, fraction, int32(seed))
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIDataFrame[T](this.worker, id), nil
}

func Sample[T any](this *IDataFrame[T], withReplacement bool, fraction float64, seed int) (*IDataFrame[T], error) {
	return this.Sample(withReplacement, fraction, seed)
}

func (this *IDataFrame[T]) TakeSample(withReplacement bool, num int64, seed int) ([]T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	id, err2 := client.Services().GetDataframeService().TakeSample(context.Background(), this.id, withReplacement, num, int32(seed), tp)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return core.Collect[T](Ignis.callback.DriverContext(), id)
}

func TakeSample[T any](this *IDataFrame[T], withReplacement bool, num int64, seed int) ([]T, error) {
	return this.TakeSample(withReplacement, num, seed)
}

func (this *IDataFrame[T]) Count() (int64, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return -1, err
	}
	defer client.Free()
	n, err := client.Services().GetDataframeService().Count(context.Background(), this.id)
	if err != nil {
		return -1, derror.NewGenericIDriverError(err)
	}
	return n, nil
}

func Count[T any](this *IDataFrame[T]) (int64, error) {
	return this.Count()
}

func (this *IDataFrame[T]) Max(cmp *ISource) (T, error) {
	return this.MaxCmp(nil)
}

func Max[T any](this *IDataFrame[T], cmp *ISource) (T, error) {
	return this.Max(cmp)
}

func (this *IDataFrame[T]) MaxCmp(cmp *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	var id int64
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	if cmp == nil {
		id, err = client.Services().GetDataframeService().Max3(context.Background(), this.id, cmp.rpc(), tp)
	} else {
		id, err = client.Services().GetDataframeService().Max(context.Background(), this.id, tp)
	}
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func MaxCmp[T any](this *IDataFrame[T], cmp *ISource) (T, error) {
	return this.Max(cmp)
}

func (this *IDataFrame[T]) Min() (T, error) {
	return this.MinCmp(nil)
}

func Min[T any](this *IDataFrame[T]) (T, error) {
	return this.Min()
}

func (this *IDataFrame[T]) MinCmp(cmp *ISource) (T, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return *new(T), err
	}
	defer client.Free()
	var id int64
	tp := core.RegisterType[T](Ignis.callback.DriverContext())
	if cmp == nil {
		id, err = client.Services().GetDataframeService().Min3(context.Background(), this.id, cmp.rpc(), tp)
	} else {
		id, err = client.Services().GetDataframeService().Min(context.Background(), this.id, tp)
	}
	if err != nil {
		return *new(T), derror.NewGenericIDriverError(err)
	}
	return core.Collect1[T](Ignis.callback.DriverContext(), id)
}

func MinCmp[T any](this *IDataFrame[T], cmp *ISource) (T, error) {
	return this.MinCmp(cmp)
}

func ToPair[Key any, Value any](this *IDataFrame[ipair.IPair[Key, Value]]) *IPairDataFrame[Key, Value] {
	return newIPairDataFrame[Key, Value](this.worker, this.id)
}

type IPairDataFrame[Key any, Value any] struct {
	IDataFrame[ipair.IPair[Key, Value]]
}

func newIPairDataFrame[Key any, Value any](worker *IWorker, id *driver.IDataFrameId) *IPairDataFrame[Key, Value] {
	return &IPairDataFrame[Key, Value]{IDataFrame[ipair.IPair[Key, Value]]{worker, id}}
}

func (this *IPairDataFrame[Key, Value]) FromPair() *IDataFrame[ipair.IPair[Key, Value]] {
	return newIDataFrame[ipair.IPair[Key, Value]](this.worker, this.id)
}

func FromPair[Key any, Value any](this *IPairDataFrame[Key, Value]) *IDataFrame[ipair.IPair[Key, Value]] {
	return this.FromPair()
}

func (this *IPairDataFrame[Key, Value]) Join(other *IPairDataFrame[Key, Value], src *ISource) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().Join(context.Background(), this.id, other.id)
	} else {
		id, err = client.Services().GetDataframeService().Join3b(context.Background(), this.id, other.id, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func Join[Key any, Value any](this *IPairDataFrame[Key, Value], other *IPairDataFrame[Key, Value], src *ISource) (*IPairDataFrame[Key, Value], error) {
	return this.Join(other, src)
}

func (this *IPairDataFrame[Key, Value]) JoinN(other *IPairDataFrame[Key, Value], numPartitions int64, src *ISource) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().Join3a(context.Background(), this.id, other.id, numPartitions)
	} else {
		id, err = client.Services().GetDataframeService().Join4(context.Background(), this.id, other.id, numPartitions, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func JoinN[Key any, Value any](this *IPairDataFrame[Key, Value], other *IPairDataFrame[Key, Value], numPartitions int64, src *ISource) (*IPairDataFrame[Key, Value], error) {
	return this.JoinN(other, numPartitions, src)
}

func FlatMapValues[Key any, Value any, R any](this *IPairDataFrame[Key, Value], src *ISource) (*IPairDataFrame[Key, R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().FlatMapValues(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, R](this.worker, id), nil
}

func MapValues[Key any, Value any, R any](this *IPairDataFrame[Key, Value], src *ISource) (*IPairDataFrame[Key, R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().MapValues(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, R](this.worker, id), nil
}

func GroupByKey[Key comparable, Value any](this *IPairDataFrame[Key, Value], src *ISource) (*IPairDataFrame[Key, []Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().GroupByKey(context.Background(), this.id)
	} else {
		id, err = client.Services().GetDataframeService().GroupByKey2b(context.Background(), this.id, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, []Value](this.worker, id), nil
}

func GroupByKeyN[Key comparable, Value any](this *IPairDataFrame[Key, Value], numPartitions int64, src *ISource) (*IPairDataFrame[Key, []Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().GroupByKey2a(context.Background(), this.id, numPartitions)
	} else {
		id, err = client.Services().GetDataframeService().GroupByKey3(context.Background(), this.id, numPartitions, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, []Value](this.worker, id), nil
}

func (this *IPairDataFrame[Key, Value]) ReduceByKey(src *ISource, localReduce bool) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().ReduceByKey(context.Background(), this.id, src.rpc(), localReduce)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func ReduceByKey[Key comparable, Value any](this *IPairDataFrame[Key, Value], src *ISource, localReduce bool) (*IPairDataFrame[Key, Value], error) {
	return this.ReduceByKey(src, localReduce)
}

func (this *IPairDataFrame[Key, Value]) ReduceByKeyN(src *ISource, numPartitions int64, localReduce bool) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().ReduceByKey(context.Background(), this.id, src.rpc(), localReduce)
	} else {
		id, err = client.Services().GetDataframeService().ReduceByKey4(context.Background(), this.id, src.rpc(), numPartitions, localReduce)
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func ReduceByKeyN[Key comparable, Value any](this *IPairDataFrame[Key, Value], src *ISource, numPartitions int64, localReduce bool) (*IPairDataFrame[Key, Value], error) {
	return this.ReduceByKeyN(src, numPartitions, localReduce)
}

func AggregateByKey[Key any, Value any, R any](this *IPairDataFrame[Key, Value], zero, seqOp, combOp *ISource) (*IPairDataFrame[Key, R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if combOp == nil {
		id, err = client.Services().GetDataframeService().AggregateByKey(context.Background(), this.id, zero.rpc(), seqOp.rpc())
	} else {
		id, err = client.Services().GetDataframeService().AggregateByKey4b(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, R](this.worker, id), nil
}

func AggregateByKeyN[Key any, Value any, R any](this *IPairDataFrame[Key, Value], zero, seqOp, combOp *ISource, numPartitions int64) (*IPairDataFrame[Key, R], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if combOp == nil {
		id, err = client.Services().GetDataframeService().AggregateByKey4a(context.Background(), this.id, zero.rpc(), seqOp.rpc(), numPartitions)
	} else {
		id, err = client.Services().GetDataframeService().AggregateByKey5(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), numPartitions)
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, R](this.worker, id), nil
}

func (this *IPairDataFrame[Key, Value]) FoldByKey(zero, src *ISource) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetDataframeService().FoldByKey(context.Background(), this.id, zero.rpc(), src.rpc(), true)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func FoldByKey[Key any, Value any](this *IPairDataFrame[Key, Value], zero, src *ISource) (*IPairDataFrame[Key, Value], error) {
	return this.FoldByKey(zero, src)
}

func (this *IPairDataFrame[Key, Value]) FoldByKeyN(zero, src *ISource, numPartitions int64, localFold bool) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().FoldByKey(context.Background(), this.id, zero.rpc(), src.rpc(), localFold)
	} else {
		id, err = client.Services().GetDataframeService().FoldByKey5(context.Background(), this.id, zero.rpc(), src.rpc(), numPartitions, localFold)
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func FoldByKeyN[Key any, Value any](this *IPairDataFrame[Key, Value], zero, src *ISource, numPartitions int64, localFold bool) (*IPairDataFrame[Key, Value], error) {
	return this.FoldByKeyN(zero, src, numPartitions, localFold)
}

func (this *IPairDataFrame[Key, Value]) SortByKey(ascending bool, src *ISource) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().SortByKey(context.Background(), this.id, ascending)
	} else {
		id, err = client.Services().GetDataframeService().SortByKey3b(context.Background(), this.id, src.rpc(), ascending)
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func SortByKey[Key any, Value any](this *IPairDataFrame[Key, Value], ascending bool, src *ISource) (*IPairDataFrame[Key, Value], error) {
	return this.SortByKey(ascending, src)
}

func (this *IPairDataFrame[Key, Value]) SortByKeyN(ascending bool, numPartitions int64, src *ISource) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetDataframeService().SortByKey3a(context.Background(), this.id, ascending, numPartitions)
	} else {
		id, err = client.Services().GetDataframeService().SortByKey4(context.Background(), this.id, src.rpc(), ascending, numPartitions)
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func SortByKeyN[Key any, Value any](this *IPairDataFrame[Key, Value], ascending bool, numPartitions int64, src *ISource) (*IPairDataFrame[Key, Value], error) {
	return this.SortByKeyN(ascending, numPartitions, src)
}

func (this *IPairDataFrame[Key, Value]) Keys() ([]Key, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[Key](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Keys(context.Background(), this.id, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[Key](Ignis.callback.DriverContext(), id)
}

func Keys[Key any, Value any](this *IPairDataFrame[Key, Value]) ([]Key, error) {
	return this.Keys()
}

func (this *IPairDataFrame[Key, Value]) Values() ([]Value, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	tp := core.RegisterType[Value](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().Values(context.Background(), this.id, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return core.Collect[Value](Ignis.callback.DriverContext(), id)
}

func Values[Key any, Value any](this *IPairDataFrame[Key, Value]) ([]Value, error) {
	return this.Values()
}

func SampleByKey[Key comparable, Value any](this *IPairDataFrame[Key, Value], withReplacement bool, fraction map[Key]float64, seed int) (*IPairDataFrame[Key, Value], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	iio.AddKeyType[Key, Value]()
	src, err := AddParam(NewISource(""), "fraction", fraction)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	id, err := client.Services().GetDataframeService().SampleByKey(context.Background(), this.id, withReplacement, src.rpc(), int32(seed))
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return newIPairDataFrame[Key, Value](this.worker, id), nil
}

func CountByKey[Key comparable, Value any](this *IPairDataFrame[Key, Value]) (map[Key]int64, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	iio.AddKeyType[Key, int64]()
	tp := core.RegisterType[map[Key]int64](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().CountByKey(context.Background(), this.id, tp)
	if err != nil {
		return nil, err
	}
	maps, err := core.Collect[map[Key]int64](Ignis.callback.DriverContext(), id)
	if err != nil {
		return nil, err
	}
	result := make(map[Key]int64)
	for _, mi := range maps {
		for key, value := range mi {
			if _, found := result[key]; found {
				result[key] += value
			} else {
				result[key] = value
			}
		}
	}

	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return result, nil
}

func CountByValue[Key any, Value comparable](this *IPairDataFrame[Key, Value]) (map[Value]int64, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	iio.AddKeyType[Value, int64]()
	tp := core.RegisterType[map[Value]int64](Ignis.callback.DriverContext())
	id, err := client.Services().GetDataframeService().CountByKey(context.Background(), this.id, tp)
	if err != nil {
		return nil, err
	}
	maps, err := core.Collect[map[Value]int64](Ignis.callback.DriverContext(), id)
	if err != nil {
		return nil, err
	}
	result := make(map[Value]int64)
	for _, mi := range maps {
		for key, value := range mi {
			if _, found := result[key]; found {
				result[key] += value
			} else {
				result[key] = value
			}
		}
	}

	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return result, nil
}
