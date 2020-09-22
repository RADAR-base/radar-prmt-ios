//
//  Source.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

struct Source: Equatable, Codable {
    let type: SourceType
    let id: String?
    let name: String?
    let expectedName: String?
    let attributes: [String: String]?
}

extension Source: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}
