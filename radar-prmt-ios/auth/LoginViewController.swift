//
//  LoginViewController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import UIKit
import RxSwift

class LoginViewController: UIViewController {
    let disposeBag = DisposeBag()

    override func viewWillAppear(_ animated: Bool) {
        navigationItem.hidesBackButton = true
    }

    @IBAction func onScanQrCode(_ sender: Any) {
        QRCodeViewController.requestAuthorization()
            .subscribeOn(MainScheduler.instance)
            .subscribe(onSuccess: { [weak self] granted in
                guard let self = self else { return }
                if granted {
                    self.performSegue(withIdentifier: "qrCodeScanner", sender: self)
                } else {
                    let alert = UIAlertController(title: "Camera access denied", message: "Camera permissions are turned off for the app. To scan a QR code, please enable these permissions.", preferredStyle: .actionSheet)
                    if let settingsUrl = NSURL(string: UIApplication.openSettingsURLString) as URL? {
                        alert.addAction(UIAlertAction(title: "Review permissions", style: .default, handler: { action in
                            UIApplication.shared.open(settingsUrl, options: [:], completionHandler: nil)
                            }))
                    }

                    alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
                    alert.addAction(UIAlertAction(title: "Enter token manually", style: .default, handler: { [weak self] action in
                        self?.performSegue(withIdentifier: "enterToken", sender: self)
                    }))
                    alert.show(self, sender: sender)
                }
            })
            .disposed(by: disposeBag)
    }
}
