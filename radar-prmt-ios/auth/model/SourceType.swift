//
//  SourceType.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

struct SourceType: Equatable, Codable {
    let id: Int64
    let producer: String
    let model: String
    let version: String
    let canRegisterDynamically: Bool
}

extension SourceType: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}
