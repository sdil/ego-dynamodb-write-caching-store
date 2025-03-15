# eGo DynamoDB Write Caching

This is a [ego](https://github.com/Tochemey/ego) DynamoDB durable state plugin/adapter that support [write caching](https://www.flashbay.com.my/blog/write-caching) which means writes are acknowledged and states are updated before the writes persisted to the store. The writes will be flushed to the store at an interval.

Using this method, the writes are fast but the tradeoff is the data might be lost before the next flush interval comes if the application panics or does not exits cleanly. This adapter will flush the writes on `SIGTERM` and `SIGINT` signals.

## Use cases

- IoT data

## How to use

```go
<TBA>
```
