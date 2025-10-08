package spubtream

//func Test_Queue(t *testing.T) {
//	qin := make(chan *Subscription[int])
//	qout := make(chan *Subscription[int])
//
//	go loop(qin, qout)
//
//	subs := make([]*Subscription[int], 3)
//
//	subs[0] = &Subscription[int]{receiver: 1}
//	subs[1] = &Subscription[int]{receiver: 2, next: subs[0]}
//	subs[2] = &Subscription[int]{receiver: 3, next: subs[1]}
//
//	qin <- subs[2]
//
//	//for i := range subs {
//	//	subs[i] = &Subscription[int]{receiver: i + 1}
//	//}
//	//for _, sub := range subs {
//	//	qin <- sub
//	//}
//
//	for sub := range qout {
//		fmt.Println(sub.receiver)
//	}
//}
