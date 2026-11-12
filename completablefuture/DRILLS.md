# CompletableFuture 練習題：Kafka share consumer 的 bug 模式

來源：apache/kafka `trunk` commit 8f4cc1d800，share consumer（KIP-932）client 端。
程式碼目錄：`/Users/unknowntpo/repo/unknowntpo/kafka/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/`

架構：兩個執行緒。app thread 呼叫 API，network thread 處理請求。兩者用 event queue 傳 `CompletableFuture` 溝通。

## 帶練習的方式

每題都照這三步：

1. 讀「真實程式碼」，自己說出 bug 在哪裡。
2. 寫「練習」中的最小重現：一個會失敗的 JUnit 測試。
3. 修正程式，讓測試通過。

不要直接看答案。答錯時，只給一個提示。

---

## 第 1 題：沒有上限的 `get()`

真實程式碼（`ShareConsumerImpl.java:857`，在 `commitSync(Duration timeout)` 裡）：

```java
Timer requestTimer = time.timer(timeout.toMillis());
ShareAcknowledgeSyncEvent event = new ShareAcknowledgeSyncEvent(acks, calculateDeadlineMs(requestTimer));
applicationEventHandler.add(event);
CompletableFuture<...> commitFuture = event.future();
... = ConsumerUtils.getResult(commitFuture);   // 內部是 future.get()，沒有 timeout
```

對照（`AsyncKafkaConsumer.java:1824`）：

```java
ConsumerUtils.getResult(commitFuture, requestTimer);  // future.get(remainingMs, MILLISECONDS)
```

**Bug：** 等待能結束，只是因為 network thread 上的 reaper（`CompletableEventReaper`）會在 deadline 到時把 future 設成失敗。network thread 卡住時，`commitSync(Duration.ofSeconds(1))` 會永遠阻塞。

**觀念：**

- 「別人保證會完成我的 future」不等於「我的等待有上限」。等待的一方要有自己的上限。
- `future.get()` 和 `future.get(t, unit)` 不同。`TimeoutException` 要轉成 API 規定的例外型別。

**練習：**

1. 寫 `Client.commitSync(Duration timeout)`。它把 `CompletableFuture<Void>` 放進 `BlockingQueue`，讓另一個 worker thread 完成。
2. 測試：worker 不處理 queue，模擬卡住。斷言 `commitSync(Duration.ofMillis(200))` 在 1 秒內拋出 `TimeoutException`。
3. 先用 `future.get()` 讓測試失敗。測試要用 `assertTimeoutPreemptively`，不然測試本身會卡住。
4. 改成 `get(timeout, unit)`，讓測試通過。
5. 加分題：timeout 之後，worker 才完成 future。這個結果會到哪裡？要不要 `cancel`？

---

## 第 2 題：失敗路徑把原始 exception 換掉

真實程式碼（`ShareConsumeRequestManager.java:1196`，ack 請求失敗）：

```java
acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error), ...);
```

`Errors.forException(error)` 把 `Throwable` 轉成錯誤碼。不認得的 exception 會變成 `UNKNOWN_SERVER_ERROR`，原始的錯誤訊息和 stack trace 就不見了。

同一個檔案的 fetch 失敗路徑（`:1069-1073`）保留了原始 exception：

```java
if (error instanceof KafkaException) {
    acks.complete((KafkaException) error);
} else {
    acks.complete(Errors.UNKNOWN_SERVER_ERROR.exception());
}
```

**觀念：**

- 在 `whenComplete`、`handle`、`exceptionally` 裡轉換錯誤時，要保留 cause。
- `join()` 拋 `CompletionException`，`get()` 拋 `ExecutionException`。真正的錯誤在 `getCause()`。

**練習：**

1. 寫 `CompletableFuture<String> send()`，它以 `new IllegalStateException("disk full")` 失敗。
2. 寫兩種包裝：
   - (a) 只回傳錯誤碼 enum。
   - (b) `new MyApiException("send failed", cause)`。
3. 測試：呼叫端用 `join()` 和 `get()` 各接一次例外。斷言根本原因的訊息是 `"disk full"`。(a) 會失敗，(b) 會通過。
4. 寫 `unwrap(Throwable)` helper，處理 `CompletionException` 和 `ExecutionException`。

---

## 第 3 題：future 有 happens-before，但旁邊的共享狀態沒有

真實程式碼（`ShareFetchBuffer.java:211-216`，在 network thread 執行）：

```java
Set<Integer> bufferedNodes() {
    lock.lock();
    try {
        if (nextInLineFetch != null && (!nextInLineFetch.isConsumed() || nextInLineFetch.hasPendingAcknowledgements())) ...
```

`isConsumed()` 和 `hasPendingAcknowledgements()` 讀的是一般欄位，不是 `volatile`，還有 `TreeMap` 和 `HashMap`。app thread 修改這些欄位時，不拿這把 lock。

**Bug：** 只有讀的一方拿 lock，所以讀寫之間沒有 happens-before。network thread 可能讀到舊值，在還有確認沒送出時就關掉 session。這個後果是推測，還沒重現。

**觀念：**

- `future.complete(v)` 和 `future.get()` 之間有 happens-before。這只保證 `v`，以及 `complete` 之前寫入的資料。
- 完成 future 之後還繼續修改同一個物件，是常見的 bug。
- lock 要讀寫雙方都拿才有效。

**練習：**

1. 寫 `class Batch`，有 `boolean consumed` 和 `Map<Long, String> pending`。thread A 修改，thread B 檢查。
2. 先回答：哪一行沒有同步？可能讀到什麼錯誤結果？
3. 用 jcstress，或重複 100 萬次的測試，試著重現。重現不出來也要能說明：重現不出來不代表沒有 bug。
4. 比較三種修正方式：
   - (a) 讀寫雙方都拿 lock。
   - (b) 改成 `volatile`，或用 immutable snapshot。
   - (c) 狀態只屬於一個 thread，另一個 thread 透過 event 傳訊息。
5. 回答：Kafka 的 event 架構比較適合哪一種？為什麼？

---

## 第 4 題：不會完成的 future，以及完成兩次的 future

真實程式碼（`events/ApplicationEventProcessor.java:553-555`）：處理 `ShareAcknowledgeSyncEvent` 時，如果 request manager 不存在，程式直接 `return`，沒有完成 event 的 future。app thread 只能等 reaper 在 deadline 時把它設成失敗。同一個檔案的 OnClose 分支（`:629-633`）會呼叫 `completeExceptionally`。

正確範例（`Acknowledgements.java:132-139`）：

```java
public void complete(KafkaException acknowledgeException) {
    if (!completed) {
        if (acknowledgeException != null) {
            this.acknowledgeException = acknowledgeException;
        }
        completed = true;
    }
}
```

第一個錯誤會留下，之後的呼叫不生效。

**觀念：**

- 每個 `return` 和 `throw` 路徑都要完成 future。可以用 `try/finally`，或集中在一個 helper 裡完成。
- `complete()` 回傳 `boolean`。第二次 `complete` 會被忽略，不會拋例外，這可能把 bug 藏起來。
- 了解 `obtrudeValue` 是什麼，以及為什麼幾乎不該用。

**練習：**

1. 寫一個 handler，它有 3 個提早 `return` 的分支。找出漏掉 `complete` 的那一個。
2. 測試：每個分支都斷言 `future.isDone()`。
3. 加分題：兩個 thread 同時對同一個 future 呼叫 `complete(1)` 和 `completeExceptionally(e)`。最後的結果是什麼？誰的 `complete()` 回傳 `true`？

---

## 總結：用自己的話回答

1. 為什麼等待的一方一定要有自己的 timeout？
2. 在失敗路徑上轉換錯誤時，要保留什麼？
3. `CompletableFuture` 保證了哪些記憶體可見性？沒有保證哪些？
4. 怎麼確認每一條路徑都完成了 future？
