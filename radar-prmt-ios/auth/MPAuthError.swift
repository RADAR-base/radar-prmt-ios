//
//  MPAuthError.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

enum MPAuthError: Error {
    case unauthorized
    case tokenNotFound
    case tokenAlreadyUsed
    case invalidJwt
    case invalidBaseUrl
    case sourceConflict
    case unknownSource
    case unreferenced
}

extension MPAuthError: LocalizedError {
    var errorDescription: String? {
        switch self {
        case .unauthorized:
            return "This app is not authorized for this ManagementPortal installation"
        case .tokenAlreadyUsed:
            return "Token has already been scanned. Please generate a new token."
        case .invalidBaseUrl:
            return "Entered base URL is invalid"
        case .invalidJwt:
            return "ManagementPortal has generated an invalid token."
        case .sourceConflict:
            return "Source already exists, cannot create a new source of the same type."
        case .unknownSource:
            return "Source that is to be updated or its user does not exist anymore."
        case .unreferenced:
            return "Authorizer is no longer referenced."
        case .tokenNotFound:
            return "Token does not exist. Please ensure that it is spelled correctly."
        }
    }
}
