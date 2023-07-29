package main

import "6.824/mr"
import "fmt"

func main() {
	ll := mr.NewLinkedList()

	fmt.Println("Is the linked list empty?", ll.IsEmpty()) // true

	ll.InsertAtHead(10)
	ll.InsertAtHead(20)
	ll.InsertAtHead(30)

	fmt.Println("Is the linked list empty?", ll.IsEmpty()) // false

	fmt.Println("Deleted node:", ll.DeleteAtHead())        // Deleted node: 30
	fmt.Println("Deleted node:", ll.DeleteAtHead())        // Deleted node: 20
	fmt.Println("Deleted node:", ll.DeleteAtHead())        // Deleted node: 10
	fmt.Println("Deleted node:", ll.DeleteAtHead())        // Deleted node: <nil>
	fmt.Println("Is the linked list empty?", ll.IsEmpty()) // true
}
