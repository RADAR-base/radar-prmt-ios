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
import JWTDecode

struct User: Codable, Equatable {
    let baseUrl: URL
    let privacyPolicyUrl: URL
    let projectId: String
    let userId: String
    let requiresUserMetadata: Bool
    var privacyPolicyAccepted: Bool = false
    var sources: [Source]? = nil
    var sourceTypes: [SourceType]? = nil
}

extension User: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(userId)
        if let sources = self.sources {
            hasher.combine(sources)
        } else {
            hasher.combine(0)
        }
    }
}

struct OAuthToken: Codable {
    let userId: String
    let accessToken: String?
    let refreshToken: String
    let expiresAt: Date
    let sourceIds: [String]

    init(refreshToken: String, accessToken: String? = nil) throws {
        self.refreshToken = refreshToken
        self.accessToken = accessToken
        let jwt = try decode(jwt: refreshToken)
        if let subject = jwt.subject {
            userId = subject
        } else {
            throw MPAuthError.invalidJwt
        }
        self.sourceIds = jwt.claim(name: "sources").array ?? []
        if let expiresAt = jwt.expiresAt {
            self.expiresAt = expiresAt
        } else {
            throw MPAuthError.invalidJwt
        }
    }

    var isValid: Bool {
        return accessToken != nil || expiresAt > Date()
    }
    func addAuthorization(to request: inout URLRequest) throws {
        guard let accessToken = self.accessToken else {
            throw MPAuthError.unauthorized
        }
        request.addValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
    }
}

extension OAuthToken : Equatable {
    static func ==(lhs: OAuthToken, rhs: OAuthToken) -> Bool {
        return lhs.refreshToken == rhs.refreshToken
    }
}

extension OAuthToken : Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(refreshToken)
    }
}

protocol Authorizer {
    func login(to url: URL) -> Observable<(User, OAuthToken)>
    func refresh(auth: OAuthToken) -> Observable<OAuthToken>
    func ensureRegistration(of source: Source, authorizedBy auth: OAuthToken) -> Observable<Source>
    func requestMetadata(for user: User, authorizedBy auth: OAuthToken) -> Observable<User>
}
