//
//  File.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 20/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import Security
import RxSwift

protocol Authorization {
    func addAuthorization(to: inout URLRequest)
    func invalidate()
    func ensureValid() -> Bool
    var baseUrl: URL { get }
    var privacyPolicyUrl: URL { get }
    var projectId: String { get }
    var userId: String { get }
    var isAuthorized: BehaviorSubject<Bool> { get }
    var isUpdatedSinceCoded: Bool { get }
    var requiresUserMetadata: Bool { get }
}

class OAuth : Authorization {
    var token: String? = nil
    let isAuthorized: BehaviorSubject<Bool> = BehaviorSubject(value: false)
    var baseUrl = URL(string: "https://radar-test.thehyve.net")!
    var privacyPolicyUrl = URL(string: "http://info.thehyve.nl/radar-cns-privacy-policy")!
    let requiresUserMetadata: Bool = false

    init() {
    }

    required init(from decoder: Decoder) throws {
    }

    func addAuthorization(to request: inout URLRequest) {
        request.addValue("Bearer \(token ?? "")", forHTTPHeaderField: "Authorization")
    }

    func ensureValid() -> Bool {
        if token == nil {
            token = "aa"
            DispatchQueue.global(qos: .background).asyncAfter(deadline: .now() + 1.0) { [weak self] in
                self?.isAuthorized.onNext(true)
            }
            return false
        } else {
            return true
        }
    }

    var userId = "u"
    var projectId = "p"
    func invalidate() {
        token = nil
        isAuthorized.onNext(false)
    }

    var isUpdatedSinceCoded: Bool = false

    func encode(to encoder: Encoder) throws {
    }
}
