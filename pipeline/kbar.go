package pipeline

import "time"

type Comparable[T any] interface {
	GreateThan(right Comparable[T]) bool
	GreateOrEqual(right Comparable[T]) bool
	Equal(right Comparable[T]) bool
	LessThan(right Comparable[T]) bool
	LessOrEqual(right Comparable[T]) bool
}

type Aggregator[T any] interface {
	Comparable[T]

	Timestamp() time.Time
	Value() T
}
