# matching-engine

This project is intended to give a idea of the type of code I write. I've choosen to examplify this with an implementation of a matching engine prototype.

Requirements: Java8 Junit4 Maven GitHub

Features implemented: 2 CCY pairs. Basic logic -- New order, cancel order, mass cancel Market and Limit orders are supported.

Bonus features: Lock free multi-threading

The matching engine framework is built in the following way:

Serializer -- Some sort of serializer responsible for getting messages off the wire. As a dummy I've added a String parser, but in reality this would ideally be something clever to avoid creating millions of objects.

Distributor -- The distributors only responsibility is to direct messages to the appropriate receiver queue. I have split the processing between order book sides. That is, there is one processing thread per book side. This should cater for scalablity (More currencies can be supported by just adding more cores).

Book Side Processor -- The book side processor handles all incoming request which is relevant to its book. And responds with OrderAccepts (new orders), rejects, partial fills, fills and CancelAccepts.

Benefits of this solution: Thread communication is entirely lock free and all synchronization is handled in compare-and-swap fashion. This should allow the enginge to perform without long delays for handling critical sections, context switches and so forth. Ideally the book threads would be pinned to particular processor cores on which nothing else would be scheduled by the OS. There are some drawbacks to this, some messages (cancels) must be sent to all threads, rather that operating on a global mutual shared state. But it is just one hashlookup extra per redundant thread, which I believe in most scenarios far outweigh the gain of having to synchronize and context switch.

Howto run: Either run the MatchingEngineIntegrationTest or play with the MatchingEngineRunner which has a main method.
