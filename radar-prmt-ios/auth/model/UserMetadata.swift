//
//  UserMetadata.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

struct UserMetadata: Equatable, Codable {
    let sourceTypes: [SourceType]
    let sources: [Source]
}
