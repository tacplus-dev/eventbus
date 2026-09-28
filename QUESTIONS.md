# Open questions — Admin RPC error mapping (2026-09-28)

Context: `EventBusHandler.adminError` now maps Kafka errors behind the Admin RPCs
to gRPC status codes. UnknownTopicOrPartition and GroupIdNotFound become NOT_FOUND,
InvalidTopic becomes INVALID_ARGUMENT, and everything else stays INTERNAL.
The test is `AdminErrorMappingTest`.

1. **Should GroupNotEmptyException / UnknownMemberIdException become FAILED_PRECONDITION?**
   DeleteConsumerGroup and ResetConsumerGroupOffsets fail on a group that still has
   active members. Kafka reports that as GroupNotEmpty or UnknownMemberId, and callers
   still get INTERNAL. That condition is not a not-found, already-exists or
   invalid-argument error, so it was out of scope.
   *Default taken:* left as INTERNAL.

2. **ALREADY_EXISTS is not mapped.** No live Admin RPC can produce TopicExistsException.
   DeleteTopic and UpdateTopicConfig are hard PERMISSION_DENIED, and there is no
   create RPC. *Default taken:* not mapped, because the error cannot occur.

3. **A negative `offset` in ResetConsumerGroupOffsets returns INTERNAL.** Kafka's
   `OffsetAndMetadata` throws a plain IllegalArgumentException on the client side,
   not a Kafka error. *Default taken:* left as INTERNAL. Mapping every
   IllegalArgumentException to INVALID_ARGUMENT would be too broad.

4. **`EventBusServiceHandlerImpl` looks like dead code.** It is the legacy
   `AdminGrpc.AdminImplBase` in `EventBusHandler`. It is constructed into the
   `handlerImpl` field but never registered or read, and it still maps every error
   to INTERNAL. *Default taken:* left untouched (no adjacent refactors). Should it
   be removed?
