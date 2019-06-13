# CompletableFuture PRD evaluation

## Status quo: ICompletableFuture & co

`ICompletableFuture` is the `CompletableFuture` replacement used in Hazelcast while maintaining compatibility with JDK 6. It extends Java's plain `Future`, adding reactive style `andThen` methods which allow registration and execution of callbacks upon completion of the future.

### `com.hazelcast.core.ICompletableFuture`

Adds `andThen` callback execution capability to java's plain `Future`.

### `com.hazelcast.spi.InternalCompletableFuture`

An "internal" interface (exposed via SPI though) which extends `ICompletableFuture` to add facilities `join()` (to get the result without dealing with checked exceptions) and `complete` the future.

The additions in this interface are covered by Java's `CompletableFuture` methods, so it is expected that all its usages will be flattened and there will be no need for an additional `CompletableFuture` subclass to cover this functionality.

### `com.hazelcast.util.executor.DelegatingFuture`

A delegating `InternalCompletableFuture` that ensures the future's value is deserialized (and cached).

### `com.hazelcast.executor.impl.CancellableDelegatingFuture`

A subclass of `DelegatingFuture` that allows cancelling the operation that performs this future's processing, used in the context of `ExecutorService` implementation. This functionality will have to be implemented in a `CompletableFuture` subclass to replace this one.

### `com.hazelcast.util.executor.CompletableFutureTask`

`CompletableFutureTask` is a `RunnableFuture` and an `ICompletableFuture`, used in the context of `CachedExecutorServiceDelegate`.

### `InvocationFuture`

A `Future` backed by an invocation. Resolution of an invocation's outcome and custom timeout logic is implemented here.

### Other special purpose implementations

* `CacheConfigFuture` is a special purpose future for managing visibility of `CacheConfig`s in `CacheService`
* `com.hazelcast.internal.util.futures.ChainingFuture` is used to sequentially chain futures in `invokeOnStableClusterSerially`.
* `CompletedFuture` is a future that is initialized completed with a value and ensures the result is deserialized before it is handed over to the user.
* `DurableExecutorServiceDelegateFuture` is a `DelegatingFuture` with a `taskId` used with `DurableExecutorServiceProxy`


## Random thoughts

### Exceptions an normal completion values

See also: https://github.com/hazelcast/hazelcast/pull/13284, https://github.com/hazelcast/hazelcast/issues/13138

Cleanup todo before: unify resolveAndThrowIfException (actually, is this a real issue???)

Design issue: is `ExceptionalResult` an impl detail of `AbstractInvocationFuture` (therefore private inner class)
or can it be reused (eg in `Invocation.pendingResponse` it models the same thing, because we may want to hold on to
the exceptional value until backups are done).


### Deserialization of result

With `CompletableFuture` we are unable to deserialize the completion value before further completion stages are executed.
This was a critical factor in deciding to extend our own `AbstractInvocationFuture` to implement `CompletionStage` (/ `CompletableFuture` ???). 

### Cancellation

### Extending `CompletableFuture`

`CompletableFuture` is a concrete class. Hazelcast-specific extensions have to extend this class, possibly leaking private API (`NodeEngine`!) to public implementations. For example,
`DurableExecutorServiceFuture<V> extends ICompletableFuture<V>` is public API; it will have to be converted to a concrete class extending `CompletableFuture<V>`. However in order to implement
deserialization of result, it will have to depend on `SerializationService` (current implementation actually depends on `NodeEngine`) -> potential for leaking Hazelcast internals.

Not doing this, deserialization cannot be integrated.

### Random thoughts on alternatives

Instead of using a `CompletableFuture` in our API, we could go with a `CompletionStage` & `Future` implementation (which is actually what `CompletableFuture` is).

In order to provide a pure Java API, an async method signature could look like this:

```java
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

public interface IMap<K, V> {
    <T extends Future<V> & CompletionStage<V>> T getAsync(K key);
}
```

This will make the codebase harder to read. Additionally, most of the huge `CompletableFuture` API is inherited from the `CompletionStage` interface, so we should implement all of it.




