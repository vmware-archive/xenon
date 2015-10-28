/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.dcp.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.Operation.CompletionHandler;

/**
 * The {@link OperationJoin} construct is a handler for {@link Operation#joinWith(Operation)} or
 * {@link OperationJoin#create(Operation...)}. functionality. After multiple parallel requests have
 * completed, only then will invoked all {@link CompletionHandler}s providing all operations and
 * failures as part of the execution context.
 */
public class OperationJoin {
    private static final int APPROXIMATE_EXPECTED_CAPACITY = 4;
    private final AtomicInteger count;
    private final ConcurrentHashMap<Long, Operation> operations;
    private final ConcurrentHashMap<Long, CompletionHandlerProxy> operationHandlers;
    volatile JoinedCompletionHandler joinedCompletion;
    private OperationContext opContext;

    private OperationJoin() {
        this.count = new AtomicInteger();
        this.operations = new ConcurrentHashMap<>(APPROXIMATE_EXPECTED_CAPACITY);
        this.operationHandlers = new ConcurrentHashMap<>(APPROXIMATE_EXPECTED_CAPACITY);
        this.opContext = OperationContext.getOperationContext();
    }

    /**
     * Create {@link OperationJoin} with an array of {@link Operation}s to be joined together in
     * parallel execution.
     */
    public static OperationJoin create(Operation... ops) {
        if (ops.length == 0) {
            throw new IllegalArgumentException("At least one operation to join expected");
        }

        OperationJoin joinOp = new OperationJoin();
        for (Operation op : ops) {
            joinOp.join(op);
            op.joinCompletionHandlers();
        }

        return joinOp;
    }

    /**
     * Create {@link OperationJoin} with a collection of {@link Operation}s to be joined together in
     * parallel execution.
     */
    public static OperationJoin create(Collection<Operation> ops) {
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("At least one operation to join expected");
        }

        OperationJoin joinOp = new OperationJoin();
        for (Operation op : ops) {
            joinOp.join(op);
        }

        return joinOp;
    }

    /**
     * Create {@link OperationJoin} with a stream of {@link Operation}s to be joined together in
     * parallel execution.
     */
    public static OperationJoin create(Stream<Operation> ops) {
        OperationJoin joinOp = new OperationJoin();
        ops.forEach((op) -> joinOp.join(op));

        if (joinOp.isEmpty()) {
            throw new IllegalArgumentException("At least one operation to join expected");
        }

        return joinOp;
    }

    /**
     * Send an already joined operation using the {@link ServiceClient}.
     */
    public static void sendWith(ServiceClient serviceClient, Operation joinedOp) {
        if (!joinedOp.isJoined()) {
            serviceClient.send(joinedOp);
            return;
        }

        // We prepare the joined operations for send, and temporarily hide that they are joined.
        // By removing the join handler before sending, we avoid a recursion on a local forward:
        // the client will not try to enumerate through joined operations again.

        // clone the operation list in case the caller is joining new operations while sending.
        Collection<Operation> joinedOps = new ArrayList<>(joinedOp.getJoinedOperations());
        for (Operation currentOp : joinedOps) {
            currentOp.joinCompletionHandlers();
        }

        // second pass nests a completion and resets the join so the operation appears stand-alone
        for (Operation currentOp : joinedOps) {
            Operation clone = currentOp.clone();
            OperationJoin oj = clone.resetJoinHandler(null);
            clone.nestCompletion((o, e) -> {
                clone.resetJoinHandler(oj);
                clone.transferResponseHeadersFrom(o);
                clone.setBodyNoCloning(o.getBodyRaw()).setStatusCode(o.getStatusCode());
                if (e != null) {
                    clone.fail(e);
                } else {
                    clone.complete();
                }
            });

            // the currentOp is no longer joined, so there is no risk of the client calling us back
            serviceClient.send(clone);
        }
    }

    /**
     * Send the join operations using the {@link ServiceHost}.
     */
    public void sendWith(ServiceHost host) {
        validateSendRequest(host);
        host.sendRequest(getOperations().iterator().next());
    }

    /**
     * Send the join operations using the {@link Service}.
     */
    public void sendWith(Service service) {
        validateSendRequest(service);
        service.sendRequest(getOperations().iterator().next());
    }

    /**
     * Send the join operations using the {@link ServiceClient}.
     */
    public void sendWith(ServiceClient client) {
        validateSendRequest(client);
        client.send(getOperations().iterator().next());
    }

    public OperationJoin join(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("'operation' must not be null.");
        }
        op.joinHandler = this;
        Operation existingOp = this.operations.put(op.getId(), op);

        // when re-joining the count should be incremented again even when the operations are
        // already presented.
        if (existingOp == null || this.operations.size() > this.count.get()) {
            this.count.incrementAndGet();
        }
        return this;
    }

    public OperationJoin setCompletion(JoinedCompletionHandler joinedCompletion) {
        this.joinedCompletion = joinedCompletion;
        return this;
    }

    OperationContext getOperationContext() {
        return this.opContext;
    }

    /**
     * Sets (overwrites) the operation context of this operataion join instance
     *
     * The visibility of this method is intentionally package-local. It is intended to
     * only be called by functions in this package, so that we can apply whitelisting
     * to limit the set of services that is able to set it.
     *
     * @param ctx the operation context to set.
     */
    void setOperationContext(OperationContext opContext) {
        this.opContext = opContext;
    }

    public boolean isEmpty() {
        return this.operations.isEmpty();
    }

    public Collection<Operation> getOperations() {
        return this.operations.values();
    }

    public Operation getOperation(long id) {
        return this.operations.get(id);
    }

    void addCompletionHandler(Operation op) {
        CompletionHandler completion = op.getCompletion();
        if (completion instanceof CompletionHandlerProxy) {
            CompletionHandler previousHandler = ((CompletionHandlerProxy) completion).completionHandler;
            if (this.operationHandlers.containsKey(op.getId())
                    && completion == previousHandler) {
                return;
            }
            completion = previousHandler;
        }

        CompletionHandlerProxy proxyHandler = new CompletionHandlerProxy(this, completion);
        op.setCompletion(proxyHandler);
        this.operationHandlers.put(op.getId(), proxyHandler);
    }

    private void validateSendRequest(Object sender) {
        if (sender == null) {
            throw new IllegalArgumentException("'sender' must not be null.");
        }
        if (isEmpty()) {
            throw new IllegalStateException("No joined operations to be sent.");
        }
    }

    @FunctionalInterface
    public static interface JoinedCompletionHandler {
        void handle(Map<Long, Operation> ops, Map<Long, Throwable> failures);
    }

    private static class CompletionHandlerProxy implements CompletionHandler {
        private final CompletionHandler completionHandler;
        private final OperationJoin join;
        private volatile Throwable failure;
        private volatile Operation completedOp;

        private CompletionHandlerProxy(OperationJoin joinHandler,
                CompletionHandler completionHandler) {
            this.join = joinHandler;
            this.completionHandler = completionHandler;
        }

        @Override
        public void handle(final Operation completedOp, final Throwable failure) {
            if (this.join.count.get() <= 0) {
                return;
            }

            this.failure = failure;
            this.completedOp = completedOp;
            this.join.operations.put(completedOp.getId(), completedOp);

            if (this.join.count.decrementAndGet() == 0) {
                if (this.join.joinedCompletion != null) {
                    OperationContext origContext = OperationContext.getOperationContext();
                    OperationContext.restoreOperationContext(this.join.opContext);
                    this.join.joinedCompletion
                            .handle(this.join.operations, this.join.getFailures());
                    OperationContext.restoreOperationContext(origContext);
                    return;
                }
                for (CompletionHandlerProxy proxyCompletion : this.join.operationHandlers.values()) {
                    CompletionHandler handler = proxyCompletion.completionHandler;
                    if (handler != null) {
                        AuthorizationContext origContext = OperationContext
                                .getAuthorizationContext();
                        OperationContext.setAuthorizationContext(proxyCompletion.completedOp
                                .getAuthorizationContext());
                        handler.handle(proxyCompletion.completedOp, proxyCompletion.failure);
                        OperationContext.setAuthorizationContext(origContext);
                    }
                }
            }
        }
    }

    Map<Long, Throwable> getFailures() {
        Map<Long, Throwable> failures = null;
        for (CompletionHandlerProxy proxyCompletion : this.operationHandlers.values()) {
            if (proxyCompletion.failure != null) {
                if (failures == null) {
                    failures = new HashMap<>();
                }
                failures.put(proxyCompletion.completedOp.getId(), proxyCompletion.failure);
            }
        }
        return failures;
    }

    public void fail(Throwable t) {
        if (this.joinedCompletion != null) {
            Map<Long, Throwable> errors = new HashMap<>(1);
            errors.put(this.operations.keys().nextElement(), t);
            OperationContext origContext = OperationContext.getOperationContext();
            OperationContext.restoreOperationContext(this.opContext);
            this.joinedCompletion.handle(this.operations, errors);
            OperationContext.restoreOperationContext(origContext);

        } else {
            for (Map.Entry<Long, Operation> entry : this.operations.entrySet()) {
                CompletionHandlerProxy handler = this.operationHandlers.get(entry.getKey());
                if (handler != null && handler.completedOp == null) {
                    AuthorizationContext origContext = OperationContext.getAuthorizationContext();
                    OperationContext.setAuthorizationContext(entry.getValue()
                            .getAuthorizationContext());
                    handler.handle(entry.getValue(), t);
                    OperationContext.setAuthorizationContext(origContext);
                }
            }
        }
    }
}
