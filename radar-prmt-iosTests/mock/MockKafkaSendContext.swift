//
//  MockKafkaSendContext.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
@testable import radar_prmt_ios

class MockKafkaSendContext: KafkaSendContext {
    var failedListener: ((String, Int16, String, Bool) -> ())? = nil
    var retryListener: ((String) -> ())? = nil
    var successListener: ((String) -> ())? = nil
    var serverFailureListener: ((String) -> ())? = nil
    var couldNotConnectListener: ((String, NetworkReachability.Mode) -> ())? = nil
    var didConnectListener: ((NetworkReachability.Mode) -> ())? = nil

    func didFail(for topic: String, code: Int16, message: String, recoverable: Bool) {
        failedListener?(topic, code, message, recoverable)
    }

    func mayRetry(topic: String) {
        retryListener?(topic)
    }

    func didSucceed(for topic: String) {
        successListener?(topic)
    }

    func serverFailure(for topic: String) {
        serverFailureListener?(topic)
    }

    func couldNotConnect(with topic: String, over mode: NetworkReachability.Mode) {
        availableNetworkModes.subtract(mode)
        couldNotConnectListener?(topic, mode)
    }

    func didConnect(over mode: NetworkReachability.Mode) -> NetworkReachability.Mode {
        availableNetworkModes.formUnion(mode)
        didConnectListener?(mode)
        return availableNetworkModes
    }

    var availableNetworkModes: NetworkReachability.Mode = [.wifiOrEthernet, .cellular]

    var retryServer: (at: Date, interval: TimeInterval)? = nil

    var minimumPriorityForCellular: Int = 1
}
