//
//  KafkaController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
import os.log

class KafkaController {
    let queue: DispatchQueue
    let sender: KafkaSender
    let context: KafkaSendContext
    let schemaRegistry: SchemaRegistryClient
    var interval: TimeInterval
    var maxProcessing: Int
    let auth: Authorizer
    let baseUrl: URL
    var reachability: NetworkReachability!
    var isStarted: Bool
    let reader: AvroDataExtractor
    let medium: RequestMedium
    let readContext: UploadContext

    init(baseURL: URL, reader: AvroDataExtractor, auth: Authorizer) {
        queue = DispatchQueue.global(qos: .background)
        self.reader = reader
        schemaRegistry = SchemaRegistryClient(baseUrl: baseURL)
        self.baseUrl = baseURL;
        self.auth = auth
        interval = 10
        maxProcessing = 10
        isStarted = false
        medium = FileRequestMedium(writeDirectory: URL(fileURLWithPath: NSTemporaryDirectory()))
        readContext = JsonUploadContext(auth: auth, medium: medium)
        context = DataKafkaSendContext(reader: reader, medium: medium)
        sender = KafkaSender(baseUrl: baseURL, context: context, auth: auth)
    }

    func start() {
        if self.reachability == nil {
            self.reachability = NetworkReachability(baseUrl: self.baseUrl) { [weak self] (mode: NetworkReachability.Mode) in
                self?.updateConnection(to: mode)
            }
        }

        queue.async { [weak self] in
            guard let self = self, !self.isStarted else { return }
            self.isStarted = true
            self.scheduleNext()
            self.scheduleNextRetry()
        }
    }

    private func scheduleNext() {
        queue.asyncAfter(deadline: .now() + interval) { [weak self] in
            self?.sendNext()
        }
    }


    private func scheduleNextRetry() {
        queue.asyncAfter(deadline: .now() + interval) { [weak self] in
            self?.sendNextRetry()
        }
    }

    private func updateConnection(to mode: NetworkReachability.Mode) {
        guard !mode.isEmpty else { return }
        self.queue.async { [weak self] in
            guard let self = self else { return }

            let newMode = self.context.didConnect(over: mode)
            if newMode == [.cellular, .wifiOrEthernet] {
                os_log("Network connection is available again. Restarting data uploads.")
                self.reachability.cancel()
                self.start()
            }
        }
    }

    func sendNextRetry() {
        guard auth.ensureValid(otherwiseRun: { [weak self] in
            self?.start()
        }) else {
            isStarted = false
            return
        }

        let availableModes = context.availableNetworkModes

        if availableModes.isEmpty {
            isStarted = false
            reachability.listen()
        } else if let retryServer = self.context.retryServer, retryServer.at > Date() {
            queue.asyncAfter(deadline: .now() + retryServer.at.timeIntervalSinceNow) { [weak self] in
                self?.sendNextRetry()
            }
        } else {
            let minimumPriority: Int?
            if availableModes.contains(.wifiOrEthernet) {
                minimumPriority = nil
            } else {
                reachability.listen()
                minimumPriority = context.minimumPriorityForCellular
            }
            reader.nextRetry(minimumPriority: minimumPriority) { [weak self] topic, dataGroupId in
                guard let self = self else { return }
                guard let topic = topic, let dataGroupId = dataGroupId else {
                    os_log("No stored messages", type: .debug)
                    self.scheduleNextRetry()
                    return
                }
                self.queue.async { [weak self] in
                    self?.send(topic: topic, dataGroupId: dataGroupId)
                }
            }
        }
    }

    func sendNext() {
        guard auth.ensureValid(otherwiseRun: { [weak self] in
            self?.start()
        }) else {
            isStarted = false
            return
        }

        let availableModes = context.availableNetworkModes

        if availableModes.isEmpty {
            isStarted = false
            reachability.listen()
        } else if let retryServer = self.context.retryServer, retryServer.at > Date() {
            queue.asyncAfter(deadline: .now() + retryServer.at.timeIntervalSinceNow) { [weak self] in
                self?.sendNext()
            }
        } else {
            let minimumPriority: Int?
            if availableModes.contains(.wifiOrEthernet) {
                minimumPriority = nil
            } else {
                reachability.listen()
                minimumPriority = context.minimumPriorityForCellular
            }

            reader.nextInQueue(minimumPriority: minimumPriority) { [weak self] topic, dataGroupId in
                guard let self = self else { return }
                guard let topic = topic, let dataGroupId = dataGroupId else {
                    os_log("No stored messages", type: .debug)
                    self.scheduleNext()
                    return
                }
                self.queue.async { [weak self] in
                    self?.send(topic: topic, dataGroupId: dataGroupId)
                }
            }
        }
    }

    func send(topic: String, dataGroupId: NSManagedObjectID) {
        schemaRegistry.requestSchemas(for: topic) { [weak self] pair in
            guard let self = self else { return }
            guard pair.isComplete else {
                os_log("No schema found for topic %@", topic)
                self.context.didFail(for: topic, code: 0, message: "No schema found", recoverable: true)
                return
            }
            self.reader.readRecords(from: dataGroupId, with: self.readContext, schemas: pair) { [weak self] handle, priority in
                guard let self = self,  let handle = handle else { return }
                let priority = priority ?? 0
                os_log("Sending data for topic %@", handle.topic)
                self.sender.send(handle: handle , priority: priority)
                self.queue.async {
                    self.sendNext()
                }
            }
        }
    }

    func sendRetry(topic: String, uploadId: NSManagedObjectID) {
        schemaRegistry.requestSchemas(for: topic) { [weak self] pair in
            guard let self = self else { return }
            guard pair.isComplete else {
                os_log("No schema found for topic %@", topic)
                self.context.didFail(for: topic, code: 0, message: "No schema found", recoverable: true)
                return
            }
            self.reader.retryUpload(for: uploadId, with: self.readContext, schemas: pair) { [weak self] handle, priority in
                guard let self = self,  let handle = handle else { return }
                let priority = priority ?? 0
                os_log("Sending data for topic %@", handle.topic)
                self.sender.send(handle: handle , priority: priority)
                self.queue.async {
                    self.sendNext()
                }
            }
        }
    }
}
