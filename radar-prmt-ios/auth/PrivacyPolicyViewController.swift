//
//  PrivacyPolicyViewController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 12/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import UIKit
import SafariServices
import RxSwift

class PrivacyPolicyViewController : UIViewController, SFSafariViewControllerDelegate {
    @IBOutlet weak var baseUrlLabel: UILabel!
    @IBOutlet weak var projectIdLabel: UILabel!
    @IBOutlet weak var userIdLabel: UILabel!
    let disposeBag = DisposeBag()
    var privacyPolicyUrl: URL? = nil

    override func viewDidLoad() {
        self.navigationItem.hidesBackButton = true
        appDelegate.authController.auth
            .subscribe(onNext: { [weak self] auth in
                guard let self = self, let auth = auth else { return }
                self.baseUrlLabel?.text = auth.baseUrl.absoluteString
                self.projectIdLabel?.text = auth.projectId
                self.userIdLabel?.text = auth.userId
                self.privacyPolicyUrl = auth.privacyPolicyUrl
            })
            .disposed(by: disposeBag)
    }

    @IBAction func acceptPrivacyPolicy(_ sender: Any) {
        UserDefaults.standard.set(true, forKey: "privacyPolicyAccepted")
        performSegue(withIdentifier: "acceptedPrivacyPolicy", sender: self)
    }

    @IBAction func showDataSources(_ sender: Any) {
        guard let privacyPolicyUrl = privacyPolicyUrl else { return }
        let sfView = SFSafariViewController(url: privacyPolicyUrl)
        sfView.delegate = self
        present(sfView, animated: true)
    }

    @IBAction func showPrivacyPolicy(_ sender: Any) {
        guard let privacyPolicyUrl = privacyPolicyUrl else { return }
        let sfView = SFSafariViewController(url: privacyPolicyUrl)
        sfView.delegate = self
        present(sfView, animated: true)
    }
}
