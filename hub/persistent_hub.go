package hub

type LocalPersistentHub[T any] struct {
	MemoHub[T]
}

func (hub *LocalPersistentHub[T]) sink() {

}
