package core

import "log"

type Color uint8

const (
	RED Color = 1 << iota
	BLACK
)

type Item interface {
	Less(than Item) bool
}

type Node struct {
	Parent *Node
	Left   *Node
	Right  *Node
	color  Color
	Item
}

type RBTree struct {
	NIL   *Node
	root  *Node
	count uint64
}

func NewRBTree() *RBTree {
	node := Node{nil, nil, nil, BLACK, nil}
	return &RBTree{
		NIL:   &node,
		root:  &node,
		count: 0,
	}
}

// Left Rotate
func (rbt *RBTree) LeftRotate(no *Node) {
	// Since we are doing the left rotation, the right child should *NOT* nil.
	if no.Right == nil {
		return
	}

	//          |                                  |
	//          X                                  Y
	//         / \         left rotate            / \
	//        α  Y       ------------->         X   γ
	//           / \                            / \
	//          β  γ                            α  β

	rchild := no.Right
	no.Right = rchild.Left

	if rchild.Left != nil {
		rchild.Left.Parent = no
	}

	rchild.Parent = no.Parent

	if no.Parent == nil {
		rbt.root = rchild
	} else if no == no.Parent.Left {
		no.Parent.Left = rchild
	} else {
		no.Parent.Right = rchild
	}

	rchild.Left = no

	no.Parent = rchild

}

// Right Rotate
func (rbt *RBTree) RightRotate(no *Node) {
	if no.Left == nil {
		return
	}

	//          |                                  |
	//          X                                  Y
	//         / \         right rotate           / \
	//        Y   γ      ------------->         α  X
	//       / \                                    / \
	//      α  β                                    β  γ

	lchild := no.Left
	no.Left = lchild.Right

	if lchild.Right != nil {
		lchild.Right.Parent = no
	}

	lchild.Parent = no.Parent

	if no.Parent == nil {
		rbt.root = lchild
	} else if no == no.Parent.Left {
		no.Parent.Left = lchild
	} else {
		no.Parent.Right = lchild
	}

	lchild.Right = no

	no.Parent = lchild

}

func (rbt *RBTree) Insert(no *Node) {
	x := rbt.root
	var y *Node = rbt.NIL

	for x != rbt.NIL {
		y = x
		if no.Item.Less(x.Item) {
			x = x.Left
		} else if x.Item.Less(no.Item) {
			x = x.Right
		} else {
			log.Println("that node already exist")
		}
	}

	no.Parent = y
	if y == rbt.NIL {
		rbt.root = no
	} else if no.Item.Less(y.Item) {
		y.Left = no
	} else {
		y.Right = no
	}

	rbt.count++
	rbt.insertFixup(no)

}

func (rbt *RBTree) insertFixup(no *Node) {
	for no.Parent.color == RED {
		if no.Parent == no.Parent.Parent.Left {
			y := no.Parent.Parent.Right
			if y.color == RED {
				//
				// 情形 4

				log.Println("TRACE Do Case 4 :", no.Item)

				no.Parent.color = BLACK
				y.color = BLACK
				no.Parent.Parent.color = RED
				no = no.Parent.Parent //循环向上自平衡.
			} else {
				if no == no.Parent.Right {
					//
					// 情形 5 : 反向情形
					// 直接左旋转 , 然后进行情形3(变色->右旋)
					log.Println("TRACE Do Case 5 :", no.Item)

					if no == no.Parent.Right {
						no = no.Parent
						rbt.LeftRotate(no)
					}
				}
				log.Println("TRACE Do Case 6 :", no.Item)

				no.Parent.color = BLACK
				no.Parent.Parent.color = RED
				rbt.RightRotate(no.Parent.Parent)
			}
		} else { //为父父节点右孩子情形，和左孩子一样，改下转向而已.
			y := no.Parent.Parent.Left
			if y.color == RED {
				no.Parent.color = BLACK
				y.color = BLACK
				no.Parent.Parent.color = RED
				no = no.Parent.Parent
			} else {
				if no == no.Parent.Left {
					no = no.Parent
					rbt.RightRotate(no)
				}

				no.Parent.color = BLACK
				no.Parent.Parent.color = RED
				rbt.LeftRotate(no.Parent.Parent)
			}
		}
	}
	rbt.root.color = BLACK
}
