//
//  MPOAuth.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift
import RxCocoa
import Network
import JWTDecode
import os.log

class MPAuthorizer {
    let clientId: String
    let clientSecret: String

    init(clientId: String, clientSecret: String) {
        self.clientId = clientId
        self.clientSecret = clientSecret
    }

    func metaTokenLogin(url tokenUrl: URL) -> Observable<MPOAuth> {
        var request = URLRequest(url: tokenUrl)
        request.addBasicAuthentication(identifiedBy: clientId, authenticatedWith: clientSecret)
        return URLSession.shared.rx.data(request: request)
            .map { data in
                let decoder = JSONDecoder()
                let metaToken = try decoder.decode(MetaToken.self, from: data)
                let auth = try MPOAuth(metaToken: metaToken, authorizer: self)
                let defaults = UserDefaults.standard
                defaults.set(auth.baseUrl, forKey: "baseUrl")
                let previousPrivacyPolicy = defaults.url(forKey: "privacyPolicyUrl")
                if previousPrivacyPolicy != auth.privacyPolicyUrl {
                    defaults.set(auth.privacyPolicyUrl, forKey: "privacyPolicyUrl")
                    defaults.set(false, forKey: "privacyPolicyAccepted")
                }
                return auth
            }
            .retryWhen { obsError in
                return obsError.zip(with: Observable.range(start: 1, count: 3)) { error, i throws -> Int in
                    switch (error) {
                    case let RxCocoaURLError.httpRequestFailed(response, data):
                        switch (response.statusCode) {
                        case 401, 403:
                            throw MPAuthError.unauthorized
                        case 410:
                            throw MPAuthError.tokenAlreadyUsed
                        default:
                            if let body = data {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: %@", tokenUrl.absoluteString, response.statusCode, String(data: body, encoding: .utf8) ?? "??")
                            } else {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: <no content>", tokenUrl.absoluteString, response.statusCode)
                            }
                        }
                    case is RxCocoaURLError:
                        os_log("Failed to make network call: %@", type: .error, error.localizedDescription)
                    case is MPAuthError:
                        throw error
                    case let DecodingError.dataCorrupted(context):
                        os_log("Invalid JSON: %@", type: .error, context.debugDescription)
                    case is DecodingError:
                        os_log("Failed to decode JSON: %@", type: .error, error.localizedDescription)
                    default:
                        break
                    }
                    return i
                }
            }
            .do(onError: {error in os_log("%@", type: .error, error.localizedDescription)})
    }

    func register(source: Source, auth: Authorization) -> Observable<Source> {
        let sourceUrl = auth.baseUrl.appendingPathComponent("managementportal/api/subjects/\(auth.userId)/sources")
        var request = URLRequest(url: sourceUrl)
        request.httpMethod = "POST"
        request.addValue("application/json; charset=utf-8", forHTTPHeaderField: "Content-Type")
        request.addValue("application/json", forHTTPHeaderField: "Accept")
        auth.addAuthorization(to: &request)

        let encoder = JSONEncoder()
        do {
            request.httpBody = try encoder.encode(SourceDTO(sourceId: nil, sourceTypeId: source.type.id, sourceName: source.name, expectedSourceName: nil, attributes: source.attributes))
        } catch {
            return Observable<Source>.error(error)
        }
        let decoder = JSONDecoder()
        return URLSession.shared.rx.data(request: request)
            .map { data in
                let sourceDTO = try decoder.decode(SourceDTO.self, from: data)
                return Source(type: source.type, id: sourceDTO.sourceId, name: sourceDTO.sourceName, expectedName: sourceDTO.expectedSourceName, attributes: sourceDTO.attributes)
            }
            .retryWhen { obsError in
                return obsError.zip(with: Observable<Int>.interval(.seconds(5), scheduler: ConcurrentDispatchQueueScheduler(qos: .background)).take(3)) { error, i throws -> Int in
                    switch (error) {
                    case let RxCocoaURLError.httpRequestFailed(response, data):
                        switch (response.statusCode) {
                        case 401, 403:
                            throw MPAuthError.unauthorized
                        case 409:
                            throw MPAuthError.sourceConflict
                        default:
                            if let body = data {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: %@", sourceUrl.absoluteString, response.statusCode, String(data: body, encoding: .utf8) ?? "??")
                            } else {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: <no content>", sourceUrl.absoluteString, response.statusCode)
                            }
                        }
                    case is RxCocoaURLError:
                        os_log("Failed to make network call: %@", type: .error, error.localizedDescription)
                    case is MPAuthError:
                        throw error
                    case let DecodingError.dataCorrupted(context):
                        os_log("Invalid JSON: %@", type: .error, context.debugDescription)
                    case is DecodingError:
                        os_log("Failed to decode JSON: %@", type: .error, error.localizedDescription)
                    default:
                        break
                    }
                    return i
                }
            }
            .do(onError: {error in os_log("%@", type: .error, error.localizedDescription)})
    }
}

struct SourceDTO : Codable {
    let sourceId: String?
    let sourceTypeId: String?
    let sourceName: String?
    let expectedSourceName: String?
    let attributes: [String: String]?
}

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

class MPOAuth: Authorization, Codable {
    private var refreshToken: String? = nil
    var token: String? {
        return jwt?.string
    }
    var jwt: JWT? = nil

    let requiresUserMetadata: Bool = true
    let projectId: String
    let userId: String
    let baseUrl: URL
    let tokenUrl: URL
    let privacyPolicyUrl: URL
    var authorizer: MPAuthorizer!
    let disposeBag = DisposeBag()
    private let jwtQueue = DispatchQueue(label: "MPOAuth", qos: .background)
    private var isRefreshing = false

    var isUpdatedSinceCoded: Bool

    let isAuthorized: BehaviorSubject<Bool> = BehaviorSubject(value: false)

    init(metaToken: MetaToken, authorizer: MPAuthorizer? = nil) throws {
        if let authorizer = authorizer {
            self.authorizer = authorizer
        }

        let refreshJwt = try metaToken.parseRefreshToken()

        refreshToken = refreshJwt.jwt.string
        userId = refreshJwt.userId
        projectId = refreshJwt.projectId

        baseUrl = try metaToken.parseBaseUrl()
        tokenUrl = self.baseUrl.appendingPathComponent("managementportal/oauth/token")
        privacyPolicyUrl = metaToken.parsePrivacyPolicyUrl() ?? URL(string: "http://info.thehyve.nl/radar-cns-privacy-policy")!
        isUpdatedSinceCoded = true
    }

    required init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let metaToken = try container.decode(MetaToken.self, forKey: .metaToken)

        let refreshJwt = try metaToken.parseRefreshToken()
        refreshToken = refreshJwt.jwt.string
        userId = refreshJwt.userId
        projectId = refreshJwt.projectId

        baseUrl = try metaToken.parseBaseUrl()
        tokenUrl = self.baseUrl.appendingPathComponent("managementportal/oauth/token")
        privacyPolicyUrl = metaToken.parsePrivacyPolicyUrl() ?? URL(string: "http://info.thehyve.nl/radar-cns-privacy-policy")!
        isUpdatedSinceCoded = false

        if container.contains(.accessToken) {
            let accessToken = try container.decode(String.self, forKey: .accessToken)
            jwt = try decode(jwt: accessToken)
        }
    }

    func addAuthorization(to request: inout URLRequest) {
        request.addValue("Bearer \(token ?? "")", forHTTPHeaderField: "Authorization")
    }

    @discardableResult
    func ensureValid() -> Bool {
        if let jwt = jwt, let exp = jwt.expiresAt, exp > Date() - 15*60 {
            return true
        } else {
            jwtQueue.async { [weak self] in self?.refresh() }
            return false
        }
    }

    private func refresh() {
        if (isRefreshing) {
            return
        }
        isRefreshing = true
        var request = URLRequest(url: tokenUrl)
        request.addBasicAuthentication(identifiedBy: authorizer.clientId, authenticatedWith: authorizer.clientSecret)
        request.httpMethod = "POST"
        request.setFormData([
            "grant_type": "refresh_token",
            "refresh_token": refreshToken!])

        struct TokenResponse: Codable {
            let accessToken: String
            let refreshToken: String

            enum CodingKeys: String, CodingKey {
                case accessToken = "access_token"
                case refreshToken = "refresh_token"
            }
        }

        URLSession.shared.rx.data(request: request)
            .map { (data: Data) throws -> TokenResponse in
                os_log("Received response %@", String(data: data, encoding: .utf8) ?? "??")
                let decoder = JSONDecoder()
                return try decoder.decode(TokenResponse.self, from: data)
            }
            .retryWhen { obsError in
                return obsError.zip(with: Observable.range(start: 1, count: 3)) { [weak self] error, i throws -> Int in
                    guard let self = self else { return 0 }
                    switch (error) {
                    case let RxCocoaURLError.httpRequestFailed(response, data):
                        switch (response.statusCode) {
                        case 401, 403:
                            throw MPAuthError.unauthorized
                        default:
                            if let body = data {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: %@", self.tokenUrl.absoluteString, response.statusCode, String(data: body, encoding: .utf8) ?? "??")
                            } else {
                                os_log("Failed to reach ManagementPortal %@ with status code %d: <no content>", self.tokenUrl.absoluteString, response.statusCode)
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
            .map { response in (response.refreshToken, try decode(jwt: response.accessToken)) }
            .subscribe(onNext: { [weak self] response in
                guard let self = self else { return }
                self.isUpdatedSinceCoded = true
                let (refreshToken, jwt) = response
                self.refreshToken = refreshToken
                self.jwt = jwt
                self.isAuthorized.onNext(true)
            }, onError: { [weak self] (error: Error) -> Void in
                guard let self = self else { return }
                self.isUpdatedSinceCoded = true
                self.isAuthorized.onNext(false)
                self.jwt = nil
                self.isRefreshing = false
                os_log("Failed to refresh OAuth2.0 token")
            }, onCompleted: { [weak self] in
                self?.isRefreshing = false
            })
            .disposed(by: disposeBag)
    }

    func invalidate() {
        jwtQueue.async { [weak self] in
            self?.jwt = nil
            self?.isUpdatedSinceCoded = true
            self?.isAuthorized.onNext(false)
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(MetaToken(baseUrl: baseUrl.absoluteString, refreshToken: refreshToken, privacyPolicyUrl: privacyPolicyUrl.absoluteString), forKey: .metaToken)
        if let token = self.token {
            try container.encode(token, forKey: .accessToken)
        }
        isUpdatedSinceCoded = false
    }

    enum CodingKeys: String, CodingKey {
        case metaToken
        case accessToken
    }
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
            .map { r in r[r.startIndex ..< r.endIndex - 1].joined(separator: ":") }
    }
}

extension URLRequest {
    mutating func addBasicAuthentication(identifiedBy username: String, authenticatedWith password: String) {
        let credentials = "\(username):\(password)"
            .data(using: .utf8)!
            .base64EncodedString()

        addValue("Basic \(credentials)", forHTTPHeaderField: "Authorization")
    }


    /// Populate the HTTPBody of `application/x-www-form-urlencoded` request
    ///
    /// - parameter parameters:   A dictionary of keys and values to be added to the request

    mutating func setFormData(_ form: [String : String]) {
        addValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        httpBody = form.map { (key, value) -> String in
                let encodedKey = key.addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed)!
                let encodedValue = value.addingPercentEncoding(withAllowedCharacters: .urlQueryValueAllowed)!
                return "\(encodedKey)=\(encodedValue)"
            }
            .joined(separator: "&")
            .data(using: .utf8)
    }
}

enum MPAuthError: Error {
    case unauthorized
    case tokenAlreadyUsed
    case invalidJwt
    case invalidBaseUrl
    case sourceConflict
}

extension MPAuthError: LocalizedError {
    var errorDescription: String? {
        switch self {
        case .unauthorized:
            return "This app is not authorized for this ManagementPortal installation"
        case .tokenAlreadyUsed:
            return "Token has already been scanned. Please refresh to generate a new token."
        case .invalidBaseUrl:
            return "Entered base URL is invalid"
        case .invalidJwt:
            return "ManagementPortal has generated an invalid token."
        case .sourceConflict:
            return "Source already exists, cannot create a new source of the same type."
        }
    }
}


extension CharacterSet {

    /// Character set containing characters allowed in query value as outlined in RFC 3986.
    ///
    /// RFC 3986 states that the following characters are "reserved" characters.
    ///
    /// - General Delimiters: ":", "#", "[", "]", "@", "?", "/"
    /// - Sub-Delimiters: "!", "$", "&", "'", "(", ")", "*", "+", ",", ";", "="
    ///
    /// In RFC 3986 - Section 3.4, it states that the "?" and "/" characters should not be escaped to allow
    /// query strings to include a URL. Therefore, all "reserved" characters with the exception of "?" and "/"
    /// should be percent-escaped in the query string.
    ///
    /// - parameter string: The string to be percent-escaped.
    ///
    /// - returns: The percent-escaped string.

    static let urlQueryValueAllowed: CharacterSet = {
        let generalDelimitersToEncode = ":#[]@" // does not include "?" or "/" due to RFC 3986 - Section 3.4
        let subDelimitersToEncode = "!$&'()*+,;="

        var allowed = CharacterSet.urlQueryAllowed
        allowed.remove(charactersIn: generalDelimitersToEncode + subDelimitersToEncode)

        return allowed
    }()

}
