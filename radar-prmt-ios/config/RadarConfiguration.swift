//
//  RemoteConfig.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift

protocol RadarConfiguration {
    func fetch(withDelay: TimeInterval)
    var names: Set<String> { get }
    var config: BehaviorSubject<[String: String]> { get }
    subscript(name: String) -> String? { get set }
    subscript(name: String, defaultValue: String) -> String { get }
}
