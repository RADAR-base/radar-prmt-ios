//
//  NetworkReachability.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 31/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import SystemConfiguration
import RxSwift

class NetworkReachability {
    struct Mode: OptionSet {
        let rawValue: Int

        static let cellular = Mode(rawValue: 1)
        static let wifiOrEthernet = Mode(rawValue: 2)
    }

    private let reachability: SCNetworkReachability

    // Queue where the `SCNetworkReachability` callbacks run
    private let queue: DispatchQueue

    // We use it to keep a backup of the last flags read.
    private var currentReachabilityFlags: SCNetworkReachabilityFlags?

    // Flag used to avoid starting listening if we are already listening
    private var isListening = false
    let subject: BehaviorSubject<Mode>

    init?(baseUrl: URL) {
        print("**NetworkReachability / init")

        guard let host = baseUrl.host, let reachability = SCNetworkReachabilityCreateWithName(nil, host) else {
            return nil
        }

        queue = DispatchQueue.global(qos: .background)

//        self.subject = BehaviorSubject<Mode>(value: [.cellular, .wifiOrEthernet])
        self.subject = BehaviorSubject<Mode>(value: [])
        self.reachability = reachability
    }

    func listen() {
        print("**NetworkReachability / listen")
        // Skips if we are already listening
        // Optional binding since `SCNetworkReachabilityCreateWithName` returns an optional object
        guard !isListening else { return }

        // Creates a context
        var context = SCNetworkReachabilityContext(version: 0, info: nil, retain: nil, release: nil, copyDescription: nil)
        // Sets `self` as listener object
        context.info = UnsafeMutableRawPointer(Unmanaged<NetworkReachability>.passUnretained(self).toOpaque())

        let callbackClosure: SCNetworkReachabilityCallBack? = {
            (reachability:SCNetworkReachability, flags: SCNetworkReachabilityFlags, info: UnsafeMutableRawPointer?) in
            guard let info = info else { return }

            // Gets the `Handler` object from the context info
            let handler = Unmanaged<NetworkReachability>.fromOpaque(info).takeUnretainedValue()
            handler.updateReachability(flags: flags)
        }

        // Registers the callback. `callbackClosure` is the closure where we manage the callback implementation
        if !SCNetworkReachabilitySetCallback(reachability, callbackClosure, &context) {
            // Not able to set the callback
        }

        // Sets the dispatch queue which is `DispatchQueue.main` for this example. It can be also a background queue
        if !SCNetworkReachabilitySetDispatchQueue(reachability, queue) {
            // Not able to set the queue
        }

        query()

        isListening = true
    }

    func query() {
        print("**NetworkReachability / query")

        // Runs the first time to set the current flags
        queue.async { [weak self] in
            guard let self = self else { return }

            // Resets the flags stored, in this way `checkReachability` will set the new ones
            self.currentReachabilityFlags = nil

            // Reads the new flags
            var flags = SCNetworkReachabilityFlags()
            SCNetworkReachabilityGetFlags(self.reachability, &flags)
            self.updateReachability(flags: flags)
        }
    }

    // Called inside `callbackClosure`
    private func updateReachability(flags: SCNetworkReachabilityFlags) {
        print("**NetworkReachability / updateReachability")
        if currentReachabilityFlags != flags {
            // Stores the new flags
            currentReachabilityFlags = flags
            let status: NetworkReachability.Mode
            if !flags.contains(.reachable)
                || (flags.contains(.connectionRequired) && !flags.contains(.connectionOnTraffic)) {
                status = []
            } else if flags.contains(.isWWAN) {
                status = .cellular
            } else {
                status = .wifiOrEthernet
            }
            print("==status", status)
            subject.on(.next(status))
            //subject.onNext(status)
        }
    }

    // Stops listening
    func cancel() {
        print("**NetworkReachability / cancel")

        // Skips if we are not listening
        // Optional binding since `SCNetworkReachabilityCreateWithName` returns an optional object
        guard isListening else { return }

        // Remove callback and dispatch queue
        SCNetworkReachabilitySetCallback(reachability, nil, nil)
        SCNetworkReachabilitySetDispatchQueue(reachability, nil)

        isListening = false
    }
}
