//
//  RetryDelay.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift

enum RetryDelay {
    case constant(delay: RxTimeInterval)
    case linear(base: RxTimeInterval, factor: RxTimeInterval)
    case exponential(base: RxTimeInterval, factor: RxTimeInterval, min: RxTimeInterval, max: RxTimeInterval)

    func delay(for iteration: Int) -> RxTimeInterval {
        switch self {
        case let .constant(delay: delay):
            return delay
        case let .linear(base: base, factor: factor):
            return base + factor * iteration
        case let .exponential(base: base, factor: factor, min: minVal, max: maxVal):
            let range: RxTimeInterval = min(base + factor * pow(2.0, Double(iteration)), maxVal)
            return RxTimeInterval(nanoseconds: UInt64.random(in: minVal.nanoseconds ..< range.nanoseconds))
        }
    }
}

extension RxTimeInterval {
    init(nanoseconds: UInt64) {
        switch nanoseconds {
        case 0 ..< UInt64(Int.max):
            self = .nanoseconds(Int(nanoseconds))
        case UInt64(Int.max) ..< UInt64(Int.max) * 1000:
            self = .microseconds(Int(nanoseconds / 1000))
        case UInt64(Int.max) * 1000 ..< UInt64(Int.max) * 1000000:
            self = .milliseconds(Int(nanoseconds / 1000000))
        case UInt64(Int.max) * 1000000 ..< UInt64(Int.max) * 10000000000:
            self = .seconds(Int(nanoseconds / 10000000000))
        default:
            self = .never
        }
    }

    var nanoseconds: UInt64 {
        get {
            switch self {
            case let .seconds(t):
                return UInt64(t) * 1000000000
            case let .milliseconds(t):
                return UInt64(t) * 1000000
            case let .microseconds(t):
                return UInt64(t) * 1000
            case let .nanoseconds(t):
                return UInt64(t)
            case .never:
                return DispatchTime.distantFuture.uptimeNanoseconds - DispatchTime.now().uptimeNanoseconds
            @unknown default:
                return DispatchTime.distantFuture.uptimeNanoseconds - DispatchTime.now().uptimeNanoseconds
            }
        }
    }

    static func +(lhs: RxTimeInterval, rhs: RxTimeInterval) -> RxTimeInterval {
        return RxTimeInterval(nanoseconds: lhs.nanoseconds + rhs.nanoseconds)
    }

    static func *(lhs: RxTimeInterval, rhs: Int) -> RxTimeInterval {
        return RxTimeInterval(nanoseconds: lhs.nanoseconds * UInt64(rhs))
    }

    static func *(lhs: RxTimeInterval, rhs: Double) -> RxTimeInterval {
        return RxTimeInterval(nanoseconds: UInt64(ceil(Double(lhs.nanoseconds) * rhs)))
    }
}

extension RxTimeInterval : Comparable {
    public static func <(lhs: RxTimeInterval, rhs: RxTimeInterval) -> Bool {
        return lhs.nanoseconds < rhs.nanoseconds
    }
}
