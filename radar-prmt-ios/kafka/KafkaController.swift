//
//  KafkaController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData

class KafkaController {
    let queue: DispatchQueue
    let sender: KafkaSender
    let context: KafkaSendContext
    var interval: TimeInterval
    var maxProcessing: Int
    let auth: Authorizer

    init(baseURL: URL, reader: TopicReader, auth: Authorizer) {
        queue = DispatchQueue.global(qos: .background)
        context = KafkaSendContext(reader: reader)
        sender = KafkaSender(baseUrl: baseURL, context: context, auth: auth)
        self.auth = auth
        interval = 10
        maxProcessing = 10
    }

    func start() {
        queue.asyncAfter(deadline: .now() + 10) { [weak self] in
            self?.trySend()
        }
    }

    func trySend() {
        guard auth.ensureValid(otherwiseRun: { [weak self] in
            self?.queue.async { [weak self] in self?.trySend() }
        }) else { return }
        guard context.processingGroups.count < maxProcessing else {
            start()
            return
        }

        if context.connectionFailed {
            waitForConnection()
            return
        }

        if let retryServer = self.context.retryServer, retryServer.at > Date() {
            queue.asyncAfter(deadline: .now() + retryServer.at.timeIntervalSinceNow) { [weak self] in
                self?.trySend()
            }
            return
        }

        context.reader.readNextRecords(excludingGroups: context.processingGroups) { [weak self] data in
            guard let self = self else { return }
            guard let data = data else { return }
            self.sender.send(data: data)
            self.trySend()
        }

        start()
    }

    func waitForConnection() {
        // Network Reachability API
    }
}

class KafkaSendContext {
    fileprivate let reader: TopicReader
    private let queue: DispatchQueue
    private var didRetrieve: SetCache<NSManagedObjectID>
    private var retryList: [RecordSet]
    var retryServer: (at: Date, interval: TimeInterval)?
    var connectionFailed: Bool

    init(reader: TopicReader) {
        retryList = []
        didRetrieve = SetCache()
        self.reader = reader
        queue = DispatchQueue(label: "Kafka send context", qos: .background)
        retryServer = nil
        connectionFailed = false
    }

    var processingGroups: [NSManagedObjectID] {
        var retrieved: [NSManagedObjectID]? = nil
        queue.sync {
            retrieved = [NSManagedObjectID](didRetrieve)
        }
        return retrieved!
    }

    func willProcess(cache: RecordSet) {
        queue.async { [weak self] in
            self?.didRetrieve.update(with: cache.dataGroupId, invalidateAfter: Date.distantFuture.timeIntervalSinceNow)
        }
    }

    func didFail(cache: RecordSet) {
        queue.async { [weak self] in
            self?.connectionFailed = false
            let previousInterval = self?.didRetrieve.invalidationInterval(for: cache.dataGroupId) ?? TopicReader.retryFailInterval / 2
            let nextInterval = Double.random(in: 100 ..< previousInterval * 2)
            self?.didRetrieve.update(with: cache.dataGroupId, invalidateAfter: nextInterval)
        }
    }

    func mayRetry(cache: RecordSet) {
        queue.async { [weak self] in
            self?.connectionFailed = false
            self?.didRetrieve.remove(cache.dataGroupId)
            self?.retryList.append(cache)
        }
    }

    func didSucceed(cache: RecordSet) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.connectionFailed = false
            self.retryServer = nil
            self.didRetrieve.remove(cache.dataGroupId)
            self.reader.remove(cache: cache)
        }
    }

    func retriableResult() -> RecordSet? {
        var currentResult: RecordSet?
        queue.sync {
            currentResult = retryList.popLast()
        }
        return currentResult
    }

    func serverFailure(cache: RecordSet) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.connectionFailed = false
            let previousInterval = self.retryServer?.interval ?? TopicReader.retryFailInterval / 2
            let nextInterval = Double.random(in: 100 ..< previousInterval * 2)
            self.retryServer = (at: Date().addingTimeInterval(nextInterval), interval: nextInterval)
        }
    }

    func couldNotConnect(cache: RecordSet) {
        queue.async { [weak self] in
            self?.connectionFailed = true
        }
    }

    func clean() {
        queue.async { [weak self] in
            self?.retryList = []
            self?.didRetrieve.clean()
        }
    }
}
