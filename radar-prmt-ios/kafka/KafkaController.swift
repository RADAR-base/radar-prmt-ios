//
//  KafkaController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import os.log

class KafkaController {
    let queue: DispatchQueue
    let sender: KafkaSender
    let context: KafkaSendContext
    var interval: TimeInterval
    var maxProcessing: Int
    let auth: Authorizer
    let baseUrl: URL
    var reachability: NetworkReachability!
    var isStarted: Bool

    init(baseURL: URL, reader: TopicReader, auth: Authorizer) {
        queue = DispatchQueue.global(qos: .background)
        context = KafkaSendContext(reader: reader)
        sender = KafkaSender(baseUrl: baseURL, context: context, auth: auth)
        self.baseUrl = baseURL;
        self.auth = auth
        interval = 10
        maxProcessing = 10
        isStarted = false
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
        }
    }

    private func scheduleNext() {
        queue.asyncAfter(deadline: .now() + interval) { [weak self] in
            self?.sendNext()
        }
    }

    private func updateConnection(to mode: NetworkReachability.Mode) {
        guard !mode.isEmpty else { return }
        self.queue.async { [weak self] in
            guard let self = self else { return }

            let newMode = self.context.couldConnect(to: mode)
            if newMode == [.cellular, .wifiOrEthernet] {
                os_log("Network connection is available again. Restarting data uploads.")
                self.reachability.cancel()
                self.start()
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
            context.reader.readNextRecords(minimumPriority: minimumPriority) { [weak self] data in
                guard let self = self else { return }
                if let data = data {
                    os_log("Sending data for topic %@", data.topic)
                    self.sender.send(data: data)
                    self.queue.async {
                        self.sendNext()
                    }
                } else {
                    self.scheduleNext()
                }
            }
        }
    }
}
