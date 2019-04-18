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

protocol KafkaSendContext {
    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool)
    func mayRetry(topic: String)
    func didSucceed(for topic: String)
    func serverFailure(for topic: String)
    func couldNotConnect(with topic: String, over mode: NetworkReachability.Mode)
    func didConnect(over mode: NetworkReachability.Mode) -> NetworkReachability.Mode

    var availableNetworkModes: NetworkReachability.Mode { get }
    var retryServer: (at: Date, interval: TimeInterval)? { get }
    var minimumPriorityForCellular: Int { get }
}

class DataKafkaSendContext: KafkaSendContext {
    let reader: AvroDataExtractor
    private let queue: DispatchQueue
    var retryServer: (at: Date, interval: TimeInterval)?
    private var networkModes: NetworkReachability.Mode
    var minimumPriorityForCellular: Int
    let retryFailInterval: TimeInterval = 600
    let medium: RequestMedium

    init(reader: AvroDataExtractor, medium: RequestMedium) {
        self.reader = reader
        queue = DispatchQueue(label: "Kafka send context", qos: .background)
        retryServer = nil
        networkModes = [.cellular, .wifiOrEthernet]
        minimumPriorityForCellular = 1
        self.medium = medium
    }

    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool = true) {
        queue.async { [weak self] in
            guard let self = self else { return }
            if recoverable {
                self.reader.registerUploadError(for: topic, code: code, message: message)
            } else {
                self.reader.removeUpload(for: topic, storedOn: self.medium)
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
            self.reader.removeUpload(for: topic, storedOn: self.medium)
        }
    }

    func serverFailure(for topic: String) {
        self.reader.rollbackUpload(for: topic)

        queue.async { [weak self] in
            guard let self = self else { return }
            let nextInterval = (100 ..< 86400).exponentialBackOff(from: self.retryServer?.interval, startingAt: self.retryFailInterval)
            os_log("Server failure. Cancelling requests until %{time_t}d", time_t(Date(timeIntervalSinceNow: nextInterval.backOff).timeIntervalSince1970))
            self.retryServer = (at: Date().addingTimeInterval(nextInterval.backOff), interval: nextInterval.interval)
        }
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

    func didConnect(over mode: NetworkReachability.Mode) -> NetworkReachability.Mode {
        var mode: NetworkReachability.Mode = []
        queue.sync {
            self.networkModes.formUnion(mode)
            mode = self.networkModes
        }
        return mode
    }

    func couldNotConnect(with topic: String, over mode: NetworkReachability.Mode) {
        reader.rollbackUpload(for: topic)
        queue.async { [weak self] in
            guard let self = self else { return }
            os_log("Cannot make connection. Cancelling requests until connection is available.")
            self.networkModes.subtract(mode)
        }
    }
}
