package pipeline

type Data[V any] interface {
	// value compare
	Value() V
	GreateThan(right Data[V]) bool
	GreateOrEqual(right Data[V]) bool
	Equal(right Data[V]) bool
	LessThan(right Data[V]) bool
	LessOrEqual(right Data[V]) bool
}

type Sequence[S any, V Data[V]] interface {
	// sequence order compare
	Index() S
	Value() V

	IsSame(right Sequence[S, V]) bool
	IsBefore(right Sequence[S, V]) bool
	IsAfter(right Sequence[S, V]) bool
	IsSameOrBefore(right Sequence[S, V]) bool
	IsSameOrAfter(right Sequence[S, V]) bool

	IsWaterMark() bool
}

type AggOptions interface {
	Key() string
	Value() any
}

type Aggregatorable[S any, V Data[V]] interface {
	WindowData() Sequence[S, V]

	Filter(...AggOptions) Aggregatorable[S, V]
	GroupBy(...AggOptions) Aggregatorable[S, V]
	Action(AggFunc[S, V]) Aggregatorable[S, V]
	MultiAction(...AggFunc[S, V]) Aggregatorable[S, V]
}

type AggFunc[S any, V Data[V]] func([]Sequence[S, V]) Aggregatorable[S, V]
