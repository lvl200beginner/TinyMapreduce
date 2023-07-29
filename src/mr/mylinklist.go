package mr

// Node represents a node in the linked list.
type Node struct {
	data interface{}
	next *Node
}

// LinkedList represents a singly linked list.
type LinkedList struct {
	head *Node
}

// NewLinkedList creates and returns a new linked list.
func NewLinkedList() *LinkedList {
	return &LinkedList{}
}

// IsEmpty checks if the linked list is empty.
func (ll *LinkedList) IsEmpty() bool {
	return ll.head == nil
}

func (ll *LinkedList) Clear() {
	ll.head = nil
}

// InsertAtHead inserts a new node with the given data at the head of the linked list.
func (ll *LinkedList) InsertAtHead(data interface{}) {
	newNode := &Node{
		data: data,
		next: ll.head,
	}
	ll.head = newNode
}

// DeleteAtHead deletes the node at the head of the linked list.
// It returns the data stored in the deleted node, or nil if the list is empty.
func (ll *LinkedList) DeleteAtHead() interface{} {
	if ll.IsEmpty() {
		return nil
	}

	data := ll.head.data
	ll.head = ll.head.next
	return data
}
