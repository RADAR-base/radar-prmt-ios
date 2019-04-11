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
    var retryServer: (at: Date, interval: TimeInterval)?
    private var networkModes: NetworkReachability.Mode
    var minimumPriorityForCellular: Int
    let retryFailInterval: TimeInterval = 600

    init(reader: TopicReader) {
        self.reader = reader
        queue = DispatchQueue(label: "Kafka send context", qos: .background)
        retryServer = nil
        networkModes = [.cellular, .wifiOrEthernet]
        minimumPriorityForCellular = 1
    }

    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool = true) {
        queue.async { [weak self] in
            if recoverable {
                self?.reader.registerUploadError(for: topic, code: code, message: message)
            } else {
                self?.reader.removeUpload(for: topic)
            }
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
            let nextInterval = KafkaSendContext.exponentialBackOff(from: self.retryServer?.interval, startingAt: self.retryFailInterval, ranging: 100 ..< 86400)
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

    var availableNetworkModes: NetworkReachability.Mode {
        get {
            var mode: NetworkReachability.Mode = []
            queue.sync {
                mode = self.networkModes
            }
            return mode
        }
    }

    func couldConnect(to mode: NetworkReachability.Mode) -> NetworkReachability.Mode {
        var mode: NetworkReachability.Mode = []
        queue.sync {
            self.networkModes.formUnion(mode)
            mode = self.networkModes
        }
        return mode
    }

    func couldNotConnect(with topic: String, to mode: NetworkReachability.Mode) {
        reader.rollbackUpload(for: topic)
        queue.async { [weak self] in
            guard let self = self else { return }
            os_log("Cannot make connection. Cancelling requests until connection is available.")
            self.networkModes.subtract(mode)
        }
    }
}
