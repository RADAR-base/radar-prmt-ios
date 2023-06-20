//
//  MPMetaToken.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import JWTDecode

struct MetaToken: Codable {
    let baseUrl: String
    let refreshToken: String?
    let privacyPolicyUrl: String?

    func parseBaseUrl() throws -> URL {
        guard let baseUrl = URL(string: self.baseUrl) else {
            throw MPAuthError.invalidBaseUrl
        }
        return baseUrl
    }

    func parseRefreshToken() throws -> MPJWT {
        guard let refreshToken = refreshToken else {
            throw MPAuthError.invalidJwt
        }
        let refreshJwt = try decode(jwt: refreshToken)
        guard let userId = refreshJwt.subject,
            let projectId = refreshJwt.parseParticipatingProject() else {
                throw MPAuthError.invalidJwt
        }
        return MPJWT(jwt: refreshJwt, userId: userId, projectId: projectId)
    }

    func parsePrivacyPolicyUrl() -> URL? {
        guard let urlString = privacyPolicyUrl else {
            return nil
        }
        return URL(string: urlString)
    }
}

struct MPJWT {
    let jwt: JWT
    let userId: String
    let projectId: String
}

extension JWT {
    var roles: [String]? {
        return claim(name: "roles").array
    }
    var sources: [String]? {
        return claim(name: "sources").array
    }

    func parseParticipatingProject() -> String? {
        return roles?
            .map { $0.split(separator: ":") }
            .first { $0.last == "ROLE_PARTICIPANT" }
            .map { role in role[role.startIndex ..< role.endIndex - 1].joined(separator: ":") }
    }
}
