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
import RxSwift

protocol KafkaSendContext {
    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool)
    func mayRetry(topic: String)
    func didSucceed(for topic: String)
    func serverFailure(for topic: String, message: String?)
    func couldNotConnect(with topic: String, over mode: NetworkReachability.Mode)
    func didConnect(over mode: NetworkReachability.Mode) -> NetworkReachability.Mode

    var availableNetworkModes: NetworkReachability.Mode { get }
    var retryServer: (at: Date, interval: TimeInterval)? { get }
    var minimumPriorityForCellular: Int { get }
    var lastEvent: BehaviorSubject<KafkaEvent> { get }
}

class DataKafkaSendContext: KafkaSendContext {
    let reader: AvroDataExtractor
    private let queue: DispatchQueue
    var retryServer: (at: Date, interval: TimeInterval)?
    private var networkModes: NetworkReachability.Mode
    var minimumPriorityForCellular: Int
    let retryFailInterval: TimeInterval = 600
    let medium: RequestMedium
    let lastEvent = BehaviorSubject<KafkaEvent>(value: .none)

    init(reader: AvroDataExtractor, medium: RequestMedium) {
        print("**DataKafkaSendContext / init")
        self.reader = reader
        queue = DispatchQueue(label: "Kafka send context", qos: .background)
        retryServer = nil
        networkModes = [.cellular, .wifiOrEthernet]
        minimumPriorityForCellular = 1
        self.medium = medium
    }

    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool = true) {
        os_log("**Kafka request failure for topic %@: %@", type: .error, topic, message)
        self.lastEvent.onNext(.appFailure(Date(), message))
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
        print("**didSucceed", topic)
        self.lastEvent.onNext(.success(Date()))
        queue.async { [weak self] in
            guard let self = self else { return }
            self.retryServer = nil
            self.reader.removeUpload(for: topic, storedOn: self.medium)
        }
    }

    func serverFailure(for topic: String, message: String?) {
        if let message = message {
            os_log("**Kafka server failure: %@", type: .error, message)
        } else {
            os_log("**Kafka server failure")
        }
        self.lastEvent.onNext(.serverFailure(Date(), message))
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
        print("**didConnect")
        lastEvent.onNext(.connected(Date()))
        var mode: NetworkReachability.Mode = []
        queue.sync {
            self.networkModes.formUnion(mode)
            mode = self.networkModes
        }
        return mode
    }

    func couldNotConnect(with topic: String, over mode: NetworkReachability.Mode) {
        print("**couldNotConnect", topic)
        lastEvent.onNext(.disconnected(Date()))
        reader.rollbackUpload(for: topic)
        queue.async { [weak self] in
            guard let self = self else { return }
            os_log("Cannot make connection. Cancelling requests until connection is available.")
            self.networkModes.subtract(mode)
        }
    }
}

enum KafkaEvent {
    case none
    case connected(Date)
    case disconnected(Date)
    case success(Date)
    case serverFailure(Date, String?)
    case appFailure(Date, String)
}
