//
//  MPAuthorizer.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxCocoa
import RxSwift
import JWTDecode
import os.log

class MPClient {
    let clientId: String
    let clientSecret: String
    let queue: SchedulerType = ConcurrentDispatchQueueScheduler(qos: .background)
    let controlQueue: SchedulerType = SerialDispatchQueueScheduler(qos: .background)
    weak var controller: AuthController?

    init(controller: AuthController, clientId: String, clientSecret: String) {
        self.controller = controller
        self.clientId = clientId
        self.clientSecret = clientSecret
    }

    func metaTokenLogin(url tokenUrl: URL) -> Observable<(User, OAuthToken)> {
        var request = URLRequest(url: tokenUrl)
        request.addBasicAuthentication(identifiedBy: clientId, authenticatedWith: clientSecret)
        os_log("Making request to %s using credentials %s:%s", tokenUrl.absoluteString, clientId, clientSecret)
        return URLSession.shared.rx.data(request: request)
            .subscribeOn(queue)
            .map { data in
                os_log("Got MetaToken response")
                let decoder = JSONDecoder()
                let metaToken = try decoder.decode(MetaToken.self, from: data)
                let auth: OAuthToken
                if let refreshToken = metaToken.refreshToken {
                    auth = try OAuthToken(refreshToken: refreshToken)
                } else {
                    throw MPAuthError.invalidJwt
                }
                let refreshJwt = try metaToken.parseRefreshToken()
                let baseUrl = try metaToken.parseBaseUrl()
                let privacyPolicyUrl = metaToken.parsePrivacyPolicyUrl() ?? URL(string: "http://info.thehyve.nl/radar-cns-privacy-policy")!
                let user = User(baseUrl: baseUrl, privacyPolicyUrl: privacyPolicyUrl, projectId: refreshJwt.projectId, userId: refreshJwt.userId, requiresUserMetadata: true, privacyPolicyAccepted: false, sources: nil, sourceTypes: nil)

                return (user, auth)
            }
//            .retryWhen { [weak self] obsError in
//                return self?.handleRetries(of: obsError, upToCount: 3, delay: .exponential(base: .seconds(5), factor: .seconds(5), min: .seconds(5), max: .seconds(1800))) { response, data in
//                    switch (response.statusCode) {
//                    case 401, 403:
//                        throw MPAuthError.unauthorized
//                    case 404:
//                        throw MPAuthError.tokenNotFound
//                    case 410:
//                        throw MPAuthError.tokenAlreadyUsed
//                    default:
//                        break
//                    }
//                    } ?? Observable<Int>.error(MPAuthError.unreferenced)
//        }
    }

    func refresh(for user: User, auth: OAuthToken) -> Observable<OAuthToken> {
        let tokenUrl = user.baseUrl.appendingPathComponent("managementportal/oauth/token")
        var request = URLRequest(url: tokenUrl)
        request.addBasicAuthentication(identifiedBy: clientId, authenticatedWith: clientSecret)
        request.postForm([
            "grant_type": "refresh_token",
            "refresh_token": auth.refreshToken])

        return URLSession.shared.rx.data(request: request)
            .map { (data: Data) throws -> OAuthToken in
                os_log("Received response %@", String(data: data, encoding: .utf8) ?? "??")
                let decoder = JSONDecoder()
                let response = try decoder.decode(TokenResponse.self, from: data)
                return try OAuthToken(refreshToken: response.refreshToken, accessToken: response.accessToken)
            }
            .retryWhen { obsError in
                return obsError.zip(with: Observable.range(start: 1, count: 3)) { error, i throws -> Int in
                    switch (error) {
                    case let RxCocoaURLError.httpRequestFailed(response, data):
                        switch (response.statusCode) {
                        case 401, 403:
                            throw MPAuthError.unauthorized
                        default:
                            if let body = data {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: %@", tokenUrl.absoluteString, response.statusCode, String(data: body, encoding: .utf8) ?? "??")
                            } else {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: <no content>", tokenUrl.absoluteString, response.statusCode)
                            }
                        }
                    case is RxCocoaURLError:
                        os_log("Failed to make network call: %@", error.localizedDescription)
                    case is MPAuthError:
                        throw error
                    case let DecodingError.dataCorrupted(context):
                        os_log("Invalid JSON: %@", context.debugDescription)
                    case is DecodingError:
                        os_log("Failed to decode JSON: %@", error.localizedDescription)
                    default:
                        break
                    }
                    return i
                }
            }
            .do(onError: {error in os_log("%@", error.localizedDescription)})
    }

    func ensureRegistration(of source: Source, for user: User, authorizedBy auth: OAuthToken) -> Observable<Source> {
        if source.id == nil {
            print("**! register ensureRegistration")
            return register(source: source, for: user, auth: auth)
        } else {
            return update(source: source, for: user, auth: auth)
        }
    }

    func requestMetadata(for user: User, authorizedBy auth: OAuthToken) throws -> Observable<User> {
        //print("**requestMetadata 1")
        struct ProjectDTO: Codable {
            let sourceTypes: [SourceTypeDTO]
        }
        struct SubjectDTO: Codable {
            let project: ProjectDTO
            let sources: [SourceDTO]
        }
        struct SourceTypeDTO: Codable {
            let id: Int64
            let producer: String
            let model: String
            let catalogVersion: String
            let canRegisterDynamically: Bool
        }

        let subjectUrl = user.baseUrl.appendingPathComponent("managementportal/api/subjects/\(user.userId)")
        var request = URLRequest(url: subjectUrl)
        try auth.addAuthorization(to: &request)
        //print("**requestMetadata 2")
        return URLSession.shared.rx.data(request: request)
            .subscribeOn(queue)
            .map { data in
                let decoder = JSONDecoder()
                let subjectDto: SubjectDTO = try decoder.decode(SubjectDTO.self, from: data)
                var user = user
                //print("**requestMetadata 3 /", user)
                user.sourceTypes = subjectDto.project.sourceTypes.map { typeDto in
                    SourceType(id: typeDto.id, producer: typeDto.producer, model: typeDto.model, version: typeDto.catalogVersion, canRegisterDynamically: typeDto.canRegisterDynamically)
                }
                user.sources = subjectDto.sources.compactMap { sourceDto in
                    guard let sourceType = user.sourceTypes?.first(where: { $0.id == sourceDto.sourceTypeId }) else {
                        return nil
                    }
                    return Source(type: sourceType, id: sourceDto.sourceId, name: sourceDto.sourceName, expectedName: sourceDto.expectedSourceName, attributes: sourceDto.attributes)
                }
                return user
            }
//            .retryWhen { [weak self] (obsError: Observable<Error>) -> Observable<Int> in
//                return self?.handleRetries(of: obsError, upToCount: 3, delay: .exponential(base: .seconds(5), factor: .seconds(5), min: .seconds(5), max: .seconds(1800))) { response, data in
//                    switch (response.statusCode) {
//                    case 401, 403:
//                        throw MPAuthError.unauthorized
//                    case 404:
//                        throw MPAuthError.unknownSource
//                    default:
//                        break
//                    }
//                } ?? Observable<Int>.error(MPAuthError.unreferenced)
//            }
    }

    private func register(source: Source, for user: User, auth: OAuthToken) -> Observable<Source> {
        print("**!MPClient / register")
        let sourceUrl = user.baseUrl.appendingPathComponent("managementportal/api/subjects/\(user.userId)/sources")
        print("**!MPClient / register / sourceUrl", sourceUrl)
        var request = URLRequest(url: sourceUrl)
        print("**!MPClient / register / request", request)
        do {
            try auth.addAuthorization(to: &request)
            try request.postJson(SourceDTO(sourceId: nil, sourceTypeId: source.type.id, sourceName: source.name, expectedSourceName: nil, attributes: source.attributes))
            print("**!MPClient / register / do")
        } catch {
            print("**!MPClient / register / error")
            return Observable<Source>.error(error)
        }

        let decoder = JSONDecoder()
        return URLSession.shared.rx.data(request: request)
            .subscribeOn(queue)
            .map { data in
                try source.updating(withJson: data, using: decoder)
            }
//            .retryWhen { [weak self] (obsError: Observable<Error>) -> Observable<Int> in
//                return self?.handleRetries(of: obsError, upToCount: 3, delay: .exponential(base: .seconds(5), factor: .seconds(5), min: .seconds(5), max: .seconds(1800))) { response, data in
//                    switch (response.statusCode) {
//                    case 401, 403:
//                        throw MPAuthError.unauthorized
//                    case 409:
//                        throw MPAuthError.sourceConflict
//                    default:
//                        break
//                    }
//                    } ?? Observable<Int>.error(MPAuthError.unreferenced)
//        }
    }

    private func update(source: Source, for user: User, auth: OAuthToken) -> Observable<Source> {
        let sourceUrl = user.baseUrl.appendingPathComponent("managementportal/api/subjects/\(user.userId)/sources")
        var request = URLRequest(url: sourceUrl)
        do {
            try auth.addAuthorization(to: &request)
            try request.postJson(source.attributes)
        } catch {
            return Observable<Source>.error(error)
        }

        let decoder = JSONDecoder()

        return URLSession.shared.rx.data(request: request)
            .subscribeOn(queue)
            .map { data in try source.updating(withJson: data, using: decoder) }
//            .retryWhen { [weak self] (obsError: Observable<Error>) -> Observable<Int> in
//                return self?.handleRetries(of: obsError, upToCount: 3, delay: .exponential(base: .seconds(5), factor: .seconds(5), min: .seconds(5), max: .seconds(1800))) { response, data in
//                    switch (response.statusCode) {
//                    case 401, 403:
//                        throw MPAuthError.unauthorized
//                    case 404:
//                        throw MPAuthError.unknownSource
//                    default:
//                        break
//                    }
//                    } ?? Observable<Int>.error(MPAuthError.unreferenced)
//        }
    }

//    private func handleRetries(of errors: Observable<Error>, upToCount: Int, delay: RetryDelay, action: @escaping (HTTPURLResponse, Data?) throws -> Void) -> Observable<Int> {
//        return errors.enumerated().flatMap { [weak self] (enumerate: (index: Int, value: Error)) -> Observable<Int> in
//            guard let self = self else { return Observable<Int>.error(MPAuthError.unreferenced) }
//            let (index: i, value: error) = enumerate
//            guard i < 3 else { return Observable<Int>.error(error) }
//
//            switch (error) {
//            case let RxCocoaURLError.httpRequestFailed(response, data):
//                try action(response, data)
//                let urlString = response.url?.absoluteString ?? "<??>"
//                if let body = data {
//                    os_log("Failed to reach ManagementPortal %@ with status code %d: %@", urlString, response.statusCode, String(data: body, encoding: .utf8) ?? "??")
//                } else {
//                    os_log("Failed to reach ManagementPortal %@ with status code %d: <no content>", urlString, response.statusCode)
//                }
//            case is RxCocoaURLError:
//                os_log("Failed to make network call: %@", type: .error, error.localizedDescription)
//            case is MPAuthError:
//                throw error
//            case let DecodingError.dataCorrupted(context):
//                os_log("Invalid JSON: %@", type: .error, context.debugDescription)
//            case is DecodingError:
//                os_log("Failed to decode JSON: %@", type: .error, error.localizedDescription)
//            default:
//                break
//            }
//
//            return Observable<Int>.timer(delay.delay(for: i), scheduler: self.queue)
//            }.do(onError: { error in os_log("%@", type: .error, error.localizedDescription) })
//    }
}

fileprivate struct SourceDTO : Codable {
    let sourceId: String?
    let sourceTypeId: Int64?
    let sourceName: String?
    let expectedSourceName: String?
    let attributes: [String: String]?
}

fileprivate extension Source {
    func updating(withJson data: Data, using decoder: JSONDecoder) throws -> Source {
        print("**!MPClient / update / data", data)
        let sourceDTO = try decoder.decode(SourceDTO.self, from: data)
        print("**!MPClient / update / sourceDTO", sourceDTO)
        print("**!MPClient / update / type", type)
        return Source(type: type, id: sourceDTO.sourceId, name: sourceDTO.sourceName, expectedName: sourceDTO.expectedSourceName, attributes: sourceDTO.attributes)
    }
}

struct TokenResponse: Codable {
    let accessToken: String
    let refreshToken: String

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
    }
}
