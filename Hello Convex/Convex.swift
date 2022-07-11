//
//  Convex.swift
//  Hello Convex
//
//  Created by Lee Danilek on 7/10/22.
//

import Foundation

class WebSocketManager: NSObject {
    let wsUri: URL
    let onOpen: () -> Void
    let onMessage: (ServerMessage) -> Void
    var state = "connecting" // TODO
    var webSocket: URLSessionWebSocketTask?
    var queuedMessages = [String]()
    
    init(wsUri: URL, onOpen: @escaping () -> Void, onMessage: @escaping (ServerMessage) -> Void) {
        self.wsUri = wsUri
        self.onOpen = onOpen
        self.onMessage = onMessage
        super.init()
        self.subscribeToWebSocket()
    }
    
    func connect() {
        let request = URLRequest(url: wsUri)
        let session = URLSession(configuration: .default, delegate: self, delegateQueue: nil)
        let webSocket = session.webSocketTask(with: request)
        self.webSocket = webSocket
        webSocket.resume()
    }
    
    func subscribeToWebSocket() {
        if self.webSocket == nil {
            self.connect()
        }
        self.webSocket?.receive(completionHandler: { [weak self] result in
            guard let self = self else { return }
            
            switch result {
            case .failure:
                fatalError("Websocket failure")
            case .success(let webSocketTaskMessage):
                switch webSocketTaskMessage {
                case .string(let string):
                    let data = string.data(using: .utf8)!
                    print("MESSAGE \(string)")
                    let message = try! JSONDecoder().decode(ServerMessage.self, from: data)
                    self.onMessage(message)
                default:
                    fatalError("Failed. Received unknown data format. Expected String")
                }
            }
            self.subscribeToWebSocket()
        })
    }
    
    func sendMessage<T: Encodable>(json: T) {
        let data = String(decoding: try! JSONEncoder().encode(json), as: UTF8.self)
        if self.state == "connected" || (
            self.state == "connecting" && json is Connect) {
            self.sendString(data)
        } else {
            self.queuedMessages.append(data)
        }
    }
    
    func sendString(_ string: String) {
        self.webSocket?.send(.string(string), completionHandler: { err in
            if err != nil {
                fatalError("ERROR sending data \(err!)")
            } else {
                print("SUCCEEDED SENDING DATA '\(string)'")
            }
        })
    }
    
    func sendQueuedMessages() {
        for message in queuedMessages {
            self.sendString(message)
        }
        self.queuedMessages.removeAll()
    }
}

extension WebSocketManager: URLSessionWebSocketDelegate {
    func urlSession(_ session: URLSession,
                    webSocketTask: URLSessionWebSocketTask,
                    didOpenWithProtocol protocol: String?) {
        self.onOpen()
        self.state = "connected"
        self.sendQueuedMessages()
    }

    
    func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        self.webSocket = nil
    }
}

typealias QueryToken = String

func serializePathAndArgs(udfPath: String, args: [Any]) -> QueryToken {
    // TODO: args
    return udfPath
}

struct AddQuery: Codable {
    var type: String
    var queryId: Int
    var udfPath: String
    var args: [String]
}

struct QuerySetModification: Codable {
    var type: String
    var baseVersion: Int
    var newVersion: Int
    var modifications: [AddQuery]
}

struct Connect: Codable {
    var type: String
    var sessionId: String
    var connectionCount: Int
}

typealias QueryId = Int
typealias MutationId = Int

typealias Value = Int  // TODO: union types?

struct StateModification: Codable {
    var type: String
    var queryId: QueryId
    // QueryUpdated
    var value: Value?
    var logLines: [String]?
    // QueryFailed
    var errorMessage: String?
}

struct StateVersion: Codable {
    var querySet: Int
    var ts: String
    var identity: Int
}

struct ServerMessage: Codable {
    var type: String
    
    // Transition
    var startVersion: StateVersion?
    var endVersion: StateVersion?
    var modifications: [StateModification]?
    // MutationResponse
    var mutationId: Int?
    var success: Bool?
    var result: String?  // TODO: union types?
    var logLines: [String]?
    // FatalError
    var error: String?
}

struct Mutation: Codable {
    var type: String
    var mutationId: MutationId
    var udfPath: String
    var args: [Value]
}

struct QueryResult: Equatable {
    var success: Bool
    var value: Value?
    var errorMessage: String?
}
struct Query {
    var result: QueryResult?
    var udfPath: String
    var args: [String]
}
struct LocalQuery {
    var id: QueryId
    var canonicalizedUdfPath: String
    var args: [String]
    var numSubscribers: Int
}
typealias QueryResultsMap = [QueryToken: Query]
typealias ChangedQueries = [QueryToken]

class RemoteQuerySet {
    var version: StateVersion
    var remoteQuerySet: [QueryId: QueryResult]
    let queryPath: (QueryId) -> String?
    
    init(queryPath: @escaping (QueryId) -> String?) {
        self.version = StateVersion(querySet: 0, ts: "0", identity: 0)
        self.remoteQuerySet = [:]
        self.queryPath = queryPath
    }
    
    func transition(_ transition: ServerMessage) {
        // let startVersion = transition.startVersion!
        // the node package compare self.version to startVersion
        for modification in transition.modifications! {
            let queryId = modification.queryId
            switch modification.type {
            case "QueryUpdated":
                let value = modification.value!
                self.remoteQuerySet[queryId] = QueryResult(success: true, value: value, errorMessage: nil)
            case "QueryFailed":
                self.remoteQuerySet[queryId] = QueryResult(success: false, value: nil, errorMessage: modification.errorMessage!)
            case "QueryRemoved":
                self.remoteQuerySet.removeValue(forKey: queryId)
            default: break
            }
        }
        self.version = transition.endVersion!
    }
}

class LocalSyncState {
    var nextQueryId = 0
    var querySetVersion = 0
    var querySet = [QueryToken: LocalQuery]()
    var queryIdToToken = [QueryId: QueryToken]()
    
    
    func subscribe(udfPath: String) -> (QuerySetModification?, QueryToken) {
        let canonicalizedUdfPath = udfPath  // TODO
        let queryToken = serializePathAndArgs(udfPath: udfPath, args: [])
        
        var existingEntry = self.querySet[queryToken]
        if existingEntry != nil {
            existingEntry!.numSubscribers += 1
            self.querySet[queryToken] = existingEntry
            return (nil, queryToken)
        }
        let queryId = self.nextQueryId
        self.nextQueryId += 1
        let baseVersion = self.querySetVersion
        self.querySetVersion += 1
        let newVersion = self.querySetVersion
        let args = [String]()
        let query = LocalQuery(id: queryId, canonicalizedUdfPath: canonicalizedUdfPath, args: args, numSubscribers: 1)
        self.querySet[queryToken] = query
        self.queryIdToToken[queryId] = queryToken
        let add = AddQuery(
            type: "Add",
            queryId: queryId,
            udfPath: udfPath,  // TODO: canonicalize
            args: []
        )
        let modification = QuerySetModification(
            type: "ModifyQuerySet",
            baseVersion: baseVersion,
            newVersion: newVersion,
            modifications: [add]
        )
        return (modification, queryToken)
    }
    
    func queryToken(queryId: QueryId) -> QueryToken? {
        return self.queryIdToToken[queryId]
    }
    
    func queryArgs(queryId: QueryId) -> [String]? {
        let token = self.queryIdToToken[queryId];
        if token != nil {
            return self.querySet[token!]!.args
        }
        return nil
    }
    
    func queryPath(queryId: QueryId) -> String? {
        let token = self.queryIdToToken[queryId];
        if token != nil {
            return self.querySet[token!]!.canonicalizedUdfPath
        }
        return nil
    }
    
}

// We don't actually support optimistic updates -- this naming is just to match npm
class OptimisticQueryResults {
    var queryResults: QueryResultsMap
    
    init() {
        self.queryResults = [:]
    }
    
    func ingestQueryResultsFromServer(
        serverQueryResults: QueryResultsMap
    ) -> ChangedQueries {
        let oldQueryResults = self.queryResults
        self.queryResults = serverQueryResults
        var changedQueries: ChangedQueries = []
        for (queryToken, query) in self.queryResults {
            let oldQuery = oldQueryResults[queryToken]
            if oldQuery == nil || oldQuery!.result != query.result {
                changedQueries.append(queryToken)
            }
        }
        return changedQueries
    }
    
    func queryResult(queryToken: QueryToken) -> Value? {
        let query = self.queryResults[queryToken]
        if query == nil {
            return nil
        }
        let result = query!.result
        if result == nil {
            return nil
        } else if result!.success {
            return result!.value!
        } else {
            fatalError("QUERY ERROR \(result!.errorMessage!)")
        }
    }
    
}

class MutationManager {
    // TODO: args
    func request(
        udfPath: String,
        args: [Value],
        mutationId: MutationId
    ) -> Mutation {
        return Mutation(
            type: "Mutation",
            mutationId: mutationId,
            udfPath: udfPath,
            args: args
        )
    }
}

let address = "https://guiltless-armadillo-773.convex.cloud"
let version = "0.1.4"

let client = {
    return ConvexClient(address: address)
}()

func websocketURI(address: String) -> URL {
    let range = address.range(of: "://")!
    let origin = address[range.upperBound..<address.endIndex]
    let protocol_ = address[address.startIndex..<range.lowerBound]
    let wsProtocol: String
    if protocol_ == "http" {
        wsProtocol = "ws"
    } else if protocol_ == "https" {
        wsProtocol = "wss"
    } else {
        fatalError("invalid protocol \(protocol_)")
    }
    let wsURI = "\(wsProtocol)://\(origin)/api/\(version)/sync"
    return URL(string: wsURI)!
}

class ConvexClient {
    var webSocketManager: WebSocketManager? = nil
    let state: LocalSyncState
    let remoteQuerySet: RemoteQuerySet
    let optimisticQueryResults: OptimisticQueryResults
    let mutationManager = MutationManager()
    var connectionCount = 0
    let sessionId = UUID().uuidString
    var listeners = [QueryToken: [() -> Void]]()
    var nextMutationId: MutationId = 0
    
    // address - The url of your Convex deployment, typically from the `origin` property of a convex.json config file, E.g. `https://small-mouse-123.convex.cloud`.
    init(address: String) {
        self.state = LocalSyncState()
        self.remoteQuerySet = RemoteQuerySet(queryPath: {_ in nil})
        self.optimisticQueryResults = OptimisticQueryResults()
        self.webSocketManager = WebSocketManager(
            wsUri: websocketURI(address: address),
            onOpen: { [weak self] in
                guard let self = self else { return }
                self.webSocketManager?.sendMessage(json: Connect(
                    type: "Connect",
                    sessionId: self.sessionId,
                    connectionCount: self.connectionCount
                ))
                self.connectionCount+=1
            },
            onMessage: {
                serverMessage in
                print("RECEIVED DATA \(serverMessage)")
                if serverMessage.type == "Transition" {
                    self.remoteQuerySet.transition(serverMessage)
                    self.notifyOnQueryResultChanges()
                } else if serverMessage.type == "MutationResponse" {
                    
                } else if serverMessage.type == "FatalError" {
                    
                }
            }
        )
    }
    
    func notifyOnQueryResultChanges() {
        let remoteQueryResults = self.remoteQuerySet.remoteQuerySet
        var queryTokenToValue: QueryResultsMap = [:]
        for (queryId, result) in remoteQueryResults {
            let queryToken = self.state.queryToken(queryId: queryId)!;
            queryTokenToValue[queryToken] = Query(
                result: result,
                udfPath: self.state.queryPath(queryId: queryId)!,
                args: self.state.queryArgs(queryId: queryId)!
            )
        }
        let changedQueries = self.optimisticQueryResults.ingestQueryResultsFromServer(serverQueryResults: queryTokenToValue)
        self.transition(changedQueries: changedQueries)
    }
    
    func transition(changedQueries: ChangedQueries) {
        for queryToken in changedQueries {
            let callbacks = self.listeners[queryToken]
            if callbacks != nil {
                for callback in callbacks! {
                    callback()
                }
            }
        }
    }
    
    func localQueryResult(queryToken: QueryToken) -> Value? {
        return self.optimisticQueryResults.queryResult(queryToken: queryToken)
    }
    
    func subscribe(udfPath: String, onChange: @escaping (Value?) -> Void) {
        let (modification, queryToken) = self.state.subscribe(udfPath: udfPath)
        if self.listeners[queryToken] == nil {
            self.listeners[queryToken] = []
        }
        self.listeners[queryToken]?.append({
            onChange(self.localQueryResult(queryToken: queryToken))
        })
        if let modification = modification {
            self.webSocketManager?.sendMessage(json: modification)
        }
    }
    
    func mutate(udfPath: String, args: [Value]) {
        let mutationId = self.nextMutationId
        self.nextMutationId += 1
        // TODO: mutation results
        let message = self.mutationManager.request(
            udfPath: udfPath,
            args: args,
            mutationId: mutationId
        )
        self.webSocketManager?.sendMessage(json: message)
    }
}

final class ObservableInt: ObservableObject {
    @Published var value: Value? = nil;
}

func useQuery(_ udfName: String) -> ObservableInt {
    let observable = ObservableInt()
    DispatchQueue.global().async {
        let udfPath = udfName // TODO: are these the same
        client.subscribe(udfPath: udfPath, onChange: {
        newValue in
            DispatchQueue.main.async {
                print("FOUND NEW VALUE \(newValue ?? 0)")
                observable.value = newValue
            }
        })
    }
    
    return observable
}

func useMutation(_ udfName: String) -> ([Value]) -> Void {
    return {
        args in
        DispatchQueue.global().async {
            client.mutate(udfPath: udfName, args: args)
        }
    }
}
