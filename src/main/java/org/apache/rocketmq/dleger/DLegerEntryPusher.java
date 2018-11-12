package org.apache.rocketmq.dleger;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.PushEntryRequest;
import org.apache.rocketmq.dleger.protocol.PushEntryResponse;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.Pair;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerEntryPusher {

    private Logger logger = LoggerFactory.getLogger(DLegerEntryPusher.class);

    private DLegerConfig dLegerConfig;
    private DLegerStore dLegerStore;

    private MemberState memberState;

    private DLegerRpcService dLegerRpcService;



    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();


    private EntryHandler entryHandler = new EntryHandler(logger);


    private QuorumAckChecker quorumAckChecker = new QuorumAckChecker(logger);


    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    public DLegerEntryPusher(DLegerConfig dLegerConfig, MemberState memberState, DLegerStore dLegerStore, DLegerRpcService dLegerRpcService) {
        this.dLegerConfig = dLegerConfig;
        this.memberState =  memberState;
        this.dLegerStore = dLegerStore;
        this.dLegerRpcService = dLegerRpcService;
        for (String peer: memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }
    }


    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }



    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer: memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }


    private long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }


    public boolean isPendingFull(long currTerm) {
        checkTermForPendingMap(currTerm, "isPendingFull");
        return  pendingAppendResponsesByTerm.get(currTerm).size() > dLegerConfig.getMaxPendingRequestsNum();
    }


    public CompletableFuture<AppendEntryResponse> waitAck(DLegerEntry entry) {
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        if (memberState.getPeerMap().size() == 1) {
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            return CompletableFuture.completedFuture(response);
        }  else {
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            AppendFuture<AppendEntryResponse> future = new AppendFuture<>(dLegerConfig.getMaxWaitAckTimeMs());
            future.setPos(entry.getPos());
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }
            wakeUpDispatchers();
            return future;
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }


    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker", logger);
        }

        @Override
        public void doWork() {
                try {
                    if (UtilAll.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                        logger.info("[{}][{}] term={} legerBegin={} legerEnd={} committed={} watermarks={}",
                            memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLegerStore.getLegerBeginIndex(), dLegerStore.getLegerEndIndex(), dLegerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                        lastPrintWatermarkTimeMs = System.currentTimeMillis();
                    }
                    if (!memberState.isLeader()) {
                        waitForRunning(1);
                        return;
                    }
                    long currTerm = memberState.currTerm();
                    checkTermForPendingMap(currTerm, "QuorumAckChecker");
                    checkTermForWaterMark(currTerm, "QuorumAckChecker");
                    if (pendingAppendResponsesByTerm.size() > 1) {
                        for (Long term: pendingAppendResponsesByTerm.keySet()) {
                            if (term != currTerm) {
                                for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry: pendingAppendResponsesByTerm.get(term).entrySet()) {
                                    AppendEntryResponse response = new AppendEntryResponse();
                                    response.setGroup(memberState.getGroup());
                                    response.setIndex(futureEntry.getKey());
                                    response.setCode(DLegerResponseCode.TERM_CHANGED.getCode());
                                    response.setLeaderId(memberState.getLeaderId());
                                    logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                                    futureEntry.getValue().complete(response);
                                }
                                pendingAppendResponsesByTerm.remove(term);
                            }
                        }
                    }
                    if (peerWaterMarksByTerm.size() > 1) {
                        for (Long term: peerWaterMarksByTerm.keySet()) {
                            if (term != currTerm) {
                                logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                                peerWaterMarksByTerm.remove(term);
                            }
                        }
                    }
                    Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);

                    long quorumIndex = -1;
                    for (Long index: peerWaterMarks.values()) {
                        int num = 0;
                        for (Long another: peerWaterMarks.values()) {
                            if (another >= index) {
                                num++;
                            }
                        }
                        if (memberState.isQuorum(num) && index > quorumIndex) {
                            quorumIndex = index;
                        }
                    }
                    dLegerStore.updateCommittedIndex(currTerm, quorumIndex);
                    ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                    boolean needCheck = false;
                    int ackNum = 0;
                    if (quorumIndex >= 0) {
                        for (Long i = quorumIndex; i >= 0; i--) {
                            try {
                                CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                                if (future == null) {
                                    needCheck = (lastQuorumIndex != -1 && lastQuorumIndex != quorumIndex && i != lastQuorumIndex);
                                    break;
                                } else if (!future.isDone()) {
                                    AppendEntryResponse response = new AppendEntryResponse();
                                    response.setGroup(memberState.getGroup());
                                    response.setTerm(currTerm);
                                    response.setIndex(i);
                                    response.setLeaderId(memberState.getSelfId());
                                    response.setPos(((AppendFuture) future).getPos());
                                    future.complete(response);
                                }
                                ackNum++;
                            } catch (Throwable t) {
                                logger.error("Error in ack to index={} term={}", i, currTerm, t);
                            }
                        }
                    }

                    if (ackNum == 0) {
                        for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                            TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                            if (future == null) {
                                break;
                            } else if (future.isTimeOut()) {
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setCode(DLegerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                future.complete(response);
                            } else {
                                break;
                            }
                        }
                        waitForRunning(1);
                    }

                    if (UtilAll.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>>  futureEntry : responses.entrySet()) {
                            if (futureEntry.getKey() < quorumIndex) {
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(futureEntry.getKey());
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                                futureEntry.getValue().complete(response);
                                responses.remove(futureEntry.getKey());
                            }
                        }
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }
                    lastQuorumIndex = quorumIndex;
                } catch (Throwable t) {
                    DLegerEntryPusher.this.logger.error("Error in {}", getName(), t);
                    UtilAll.sleep(100);
                }
        }
    }

    private class EntryDispatcher extends ShutdownAbleThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        private long lastPushCommitTimeMs = -1;
        private String peerId;
        private long compareIndex = -1;
        private long writeIndex = -1;
        private int maxPendingSize = 1000;
        private long term = -1;
        private String leaderId =  null;
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }


        private boolean checkAndFreshState() {
            if (!memberState.isLeader()) {
                return false;
            }
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLegerResponseCode.UNKNOWN);
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }


        private PushEntryRequest buildPushRequest(DLegerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            request.setCommitIndex(dLegerStore.getCommittedIndex());
            return request;
        }


        private void doAppendInner(long index) throws Exception {
            DLegerEntry entry = dLegerStore.get(index);
            PreConditions.check(entry != null, DLegerResponseCode.UNKNOWN, "writeIndex=%d", index);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            CompletableFuture<PushEntryResponse> responseFuture = dLegerRpcService.push(request);
            pendingMap.put(index, System.currentTimeMillis());
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLegerResponseCode.UNKNOWN);
                    DLegerResponseCode responseCode = DLegerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            pendingMap.remove(x.getIndex());
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        private void doCommit() throws Exception {
            if (UtilAll.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLegerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        private void doCheckAppendResponse() throws Exception {
            long peerWaterMark =  getPeerWaterMark(term, peerId);
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLegerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }
        private void doAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > dLegerStore.getLegerEndIndex()) {
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                if (pendingMap.size() >= maxPendingSize || (UtilAll.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark =  getPeerWaterMark(term, peerId);
                    for (Long index: pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        private void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLegerResponseCode.UNKNOWN);
            DLegerEntry truncateEntry = dLegerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLegerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLegerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLegerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLegerResponseCode.SUCCESS.getCode(), DLegerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    quorumAckChecker.wakeup();
                    writeIndex =  index + 1;
                    break;
                case COMPARE:
                    if(this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        pendingMap.clear();
                    }
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }


        private void doCompare() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                if (compareIndex == -1 && dLegerStore.getLegerEndIndex() == -1) {
                    break;
                }
                if (compareIndex == -1) {
                    compareIndex = dLegerStore.getLegerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLegerStore.getLegerEndIndex() || compareIndex < dLegerStore.getLegerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLegerStore.getLegerBeginIndex(), dLegerStore.getLegerEndIndex());
                    compareIndex = dLegerStore.getLegerEndIndex();
                }
                DLegerEntry entry = dLegerStore.get(compareIndex);
                PreConditions.check(entry != null, DLegerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLegerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLegerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLegerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLegerResponseCode.SUCCESS.getCode()
                    , DLegerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                long truncateIndex = -1;
                if (response.getCode() == DLegerResponseCode.SUCCESS.getCode()) {
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLegerStore.getLegerBeginIndex()
                    || response.getBeginIndex() > dLegerStore.getLegerEndIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                } else if(compareIndex < response.getBeginIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                } else if (compareIndex > response.getEndIndex()){
                    compareIndex =  response.getEndIndex();
                } else {
                    compareIndex--;
                }
                if (compareIndex < dLegerStore.getLegerBeginIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                }
                if (truncateIndex != -1) {
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        @Override
        public void doWork() {
            try {
                if (!checkAndFreshState()) {
                    waitForRunning(1);
                    return;
                }

                if (type.get() == PushEntryRequest.Type.APPEND) {
                    doAppend();
                } else {
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                UtilAll.sleep(500);
            }
        }
    }

    private class EntryHandler extends ShutdownAbleThread {


        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler", logger);
        }

        public CompletableFuture<PushEntryResponse>  handlePush(PushEntryRequest request) throws Exception {
            CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
            switch (request.getType()) {
                case APPEND:
                    PreConditions.check(request.getEntry() != null, DLegerResponseCode.UNEXPECTED_ARGUMENT);
                    long index = request.getEntry().getIndex();
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLegerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, DLegerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLegerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }



        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getEntry().getIndex());
            }
            response.setBeginIndex(dLegerStore.getLegerBeginIndex());
            response.setEndIndex(dLegerStore.getLegerEndIndex());
            return response;
        }

        private void handleDoAppend(long writeIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLegerResponseCode.INCONSISTENT_STATE);
                DLegerEntry entry = dLegerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLegerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLegerResponseCode.SUCCESS.getCode()));
                dLegerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLegerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLegerResponseCode.UNKNOWN);
                DLegerEntry local = dLegerStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), DLegerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLegerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLegerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLegerResponseCode.UNKNOWN);
                dLegerStore.updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLegerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLegerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }


        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLegerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLegerResponseCode.UNKNOWN);
                long index = dLegerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLegerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLegerResponseCode.SUCCESS.getCode()));
                dLegerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {
                    waitForRunning(1);
                    return;
                }
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair  = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLegerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    long nextIndex = dLegerStore.getLegerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair  = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    handleDoAppend(nextIndex, request, pair.getValue());
                }
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
                UtilAll.sleep(100);
            }
        }
    }
}
