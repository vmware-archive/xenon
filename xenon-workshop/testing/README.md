
# Hands on lab work items

*Stateless Service*
- Implement `Top10BookRankingService` that returns top 10 sold books

*Subscription*
- Implement a subscription in `BookstoreDemo` that listens creations of `BookService` 

*Testing*
- Implement a test to verify top10 docs in `BookServiceTest`


# Builds

## Building this code

```shell
./mvnw -pl xenon-workshop/testing package
```

## Running the host

```shell
java -jar xenon-workshop/testing/target/workshop-testing-[VERSION]-jar-with-dependencies.jar
```

or run `BookstoreDemo` class from IDE.


## Populate data

```shell
alias curl-json='curl -H "Content-Type: application/json"';
curl-json -X POST http://localhost:8000/data/books -d "{'title':'foo','sold': 10, 'documentSelfLink':'/foo'}";
curl-json -X POST http://localhost:8000/data/books -d "{'title':'bar','sold': 3, 'documentSelfLink':'/bar'}";
curl-json -X POST http://localhost:8000/data/books -d "{'title':'baz','sold': 5, 'documentSelfLink':'/baz'}";
```
