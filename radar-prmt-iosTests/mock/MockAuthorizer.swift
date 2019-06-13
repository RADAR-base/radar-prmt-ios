//
//  MockAuthorizer.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift
@testable import radar_prmt_ios

class MockAuthorizer : Authorization {
    func ensureValid() -> Bool {
        return true
    }

    var isAuthorized: BehaviorSubject<Bool> = BehaviorSubject(value: true)

    func addAuthorization(to: inout URLRequest) {
        // no auth needed
    }
    func invalidate() {}
    func ensureValid(otherwiseRun callback: @escaping () -> Void) -> Bool { return true }
    var projectId: String { get { return "p" } }
    var userId: String { get { return "u" } }
}
