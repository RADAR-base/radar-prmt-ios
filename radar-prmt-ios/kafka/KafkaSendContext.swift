//
//  KafkaSendContext.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 31/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
import os.log

class KafkaSendContext {
    let reader: TopicReader
    private let queue: DispatchQueue
    private var failedTopics: SetCache<String>
    var retryServer: (at: Date, interval: TimeInterval)?
    var connectionFailedFor: NetworkReachability.Mode
    var minimumPriorityForCellular: Int

    init(reader: TopicReader) {
        failedTopics = SetCache()
        self.reader = reader
        queue = DispatchQueue(label: "Kafka send context", qos: .background)
        retryServer = nil
        connectionFailedFor = .none
        minimumPriorityForCellular = 1
    }

    func processingTopics() -> [String] {
        var retrieved: [String]?
        queue.sync {
            retrieved = [String](failedTopics)
        }
        return retrieved!
    }

    func didFail(for topic: String) {
        queue.async { [weak self] in
            guard let self = self else { return }
            let previousInterval = self.failedTopics.invalidationInterval(for: topic)
            let nextInterval = KafkaSendContext.exponentialBackOff(from: previousInterval, startingAt: TopicReader.retryFailInterval, ranging: 100 ..< 86400)
            os_log("Upload failure. Cancelling requests for topic %@ until %{time_t}d", topic, time_t(Date(timeIntervalSinceNow: nextInterval).timeIntervalSince1970))
            self.failedTopics.update(with: topic, invalidateAfter: nextInterval)
            self.reader.rollbackUpload(for: topic)
        }
    }

    func mayRetry(topic: String) {
        self.reader.rollbackUpload(for: topic)
    }

    func didSucceed(for topic: String) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.retryServer = nil
            self.reader.removeUpload(for: topic)
        }
    }

    func serverFailure(for topic: String) {
        self.reader.rollbackUpload(for: topic)

        queue.async { [weak self] in
            guard let self = self else { return }
            let nextInterval = KafkaSendContext.exponentialBackOff(from: self.retryServer?.interval, startingAt: TopicReader.retryFailInterval, ranging: 100 ..< 86400)
            os_log("Server failure. Cancelling requests until %{time_t}d", time_t(Date(timeIntervalSinceNow: nextInterval).timeIntervalSince1970))
            self.retryServer = (at: Date().addingTimeInterval(nextInterval), interval: nextInterval)
        }
    }

    private static func exponentialBackOff(from interval: TimeInterval?, startingAt defaultInterval: TimeInterval, ranging range: Range<TimeInterval>) -> TimeInterval {
        let nextInterval: TimeInterval
        if let interval = interval, interval <= range.upperBound {
            nextInterval = interval * 2
        } else {
            nextInterval = defaultInterval
        }
        return Double.random(in: range.lowerBound ..< min(nextInterval, range.upperBound))
    }

    func couldNotConnect(with topic: String) {
        reader.rollbackUpload(for: topic)
        queue.async { [weak self] in
            guard let self = self else { return }
            os_log("Cannot make connection. Cancelling requests until connection is available.")
            let failureMode: NetworkReachability.Mode = metadata.topic.priority < self.minimumPriorityForCellular
                ? .wifiOrEthernet
                : .cellular

            if failureMode != self.connectionFailedFor && self.connectionFailedFor != .cellular {
                self.connectionFailedFor = failureMode
            }
        }
    }

    func clean() {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.failedTopics.clean()
        }
    }
}
