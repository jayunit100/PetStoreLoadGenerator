- This repository writes bigpetstore transactions to a rotating file queue.

- You better have a consumer somewhere, otherwise it will fill up your disk very quickly !

- Its a little raw: I can clean it up later if people start getting interested.

- Also PR's are welcome !

# Get Started ! #

Clone this repository.  Then just run

```
gradlew build
```

And then you can run the jar easily.

```
 java -cp ./build/libs/PetStoreLoadGenerator-1.0.jar:libs/* org.apache.bigtop.load.LoadGen /tmp 1 5 10000 123
```

For different parameter settings you can try the following.

*Increase timescale, lower customers, for higher throughput, but unrealistic *

```
/tmp 1 5 1500000 13241234
```

*Increase customers, for more diversity*

```
/tmp 1 500000 15000 13241234
```

And so on.

Have fun ! For me, I was able to generate 25k transactions per minute.