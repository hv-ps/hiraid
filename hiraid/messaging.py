#!/usr/bin/python3.6
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.00
# -----------------------------------------------------------------------------------------------------------------------------------
#
# License Terms
# -------------
# Unless stated otherwise, Hitachi Vantara Limited and/or its group companies is/are the owner or the licensee
# of all intellectual property rights in this script. This work is protected by copyright laws and treaties around
# the world. This script is solely for use by Hitachi Vantara Limited and/or its group companies in the provision
# of services to you by Hitachi Vantara Limited and/or its group companies and, as a condition of your receiving
# such services, you expressly agree not to use, reproduce, duplicate, copy, sell, resell or exploit for any purposes,
# commercial or otherwise, this script or any portion of this script. All of Hitachi Vantara Limited and/or its
# group companies rights are reserved.
#
# -----------------------------------------------------------------------------------------------------------------------------------
# Changes:
#
# 14/01/2020    v1.1.00     Initial Release
#
# -----------------------------------------------------------------------------------------------------------------------------------

class English:
    step2 = "Please complete the following actions before executing the next step:\n\n\t1) Present (zone) Target Array paths to the host(s)\n\t2) Scan for new paths on the host\n\t3) Disable and delete Source Array paths (if applicable)\n\t4) Unzone the Source Array paths to the host(s)"
    acknowledgenextstep = "Please acknowledge that you understand the above actions and that you will complete all required tasks before proceeding to the next step"
    preactionstep3 = "Please complete the following actions before continuing this step:\n\n\t1) For this migration group confirm that Target paths are in use by the host(s).\n\t2) For this migration group confirm that the Source Paths are NOT accessible by the host(s)."
    prepairsplitRS = "pairsplit -RS ( SVOL group to SSWS )"
    endofpairsplitRS = "!! IMPORTANT !! Please now check the stability of the hosts in this migration group !!\n"
    preactionstep4 = "!! IMPORTANT !! Please confirm that the hosts in this migration group are stable."
    prepairsplitR = "pairsplit -R ( group to SMPL )"
    hurpreactionstep3 = "The legacy HUR DR pair relationship will be removed ( pairsplit -S ) in order for the GAD migration split to work.\n\nDR host should be prevented from reading or writing to the DR HUR volumes!!!\n\nPlease confirm that it is ok to proceed."
    hurprepairsplitS = "pairsplit -S ( SVOL group to SMPL )"
    prepairsplitRSDRHost = "pairsplit -RS ( SVOL group to SSWS ) DR Host"
    hurpreactionstep3 = "DR host is migrating through GAD. Please complete the following actions before continuing this step:\n\n\t1) For this migration group confirm that DR host target paths are in use by the host(s).\n\t2) For this migration group confirm that the Source Paths are NOT accessible by the host(s)."
    drpreactionstep4 = "DR host is migrating through GAD and is about to be pairsplit -RS ( SVOL group to SSWS ).\n\n\t1) For this migration group confirm that Target paths are in use by the host(s).\n\t2) For this migration group confirm that the Source Paths are NOT accessible by the host(s)."
    drpreactionstep5 = "!! IMPORTANT !! Please confirm that the DR hosts in this migration group are stable."
    endofDRpairsplitRS = "!! IMPORTANT !! Please now check the stability of the hosts in this migration group !!"

class French:
    step2 = "S'il vous plaît compléter les actions suivantes avant d'exécuter l'étape suivante:\n\n\t1) Présenter (zone) les chemins de la matrice cible vers le ou les hôtes\n\t2) Rechercher de nouveaux chemins sur l'hôte\n\t3) Désactiver et supprimer les chemins de la matrice source (le cas échéant)\n\t4) Annulez les chemins de la matrice source sur le ou les hôtes."
    acknowledgenextstep = "Veuillez reconnaître que vous comprenez les actions ci-dessus et que vous effectuerez toutes les tâches requises avant de passer à l'étape suivante."
    preactionstep3 = "S'il vous plaît compléter les actions suivantes avant d'exécuter l'étape suivante\n\n\t1) Pour ce groupe de migration, confirmez que les chemins d'accès cible sont utilisés par le ou les hôtes.\n\t2) Pour ce groupe de migration, confirmez que les chemins source ne sont PAS accessibles par le ou les hôtes."
    prepairsplitRS = "pairsplit -RS (groupe SVOL vers SSWS)"
    endofpairsplitRS = "!! IMPORTANT !! Veuillez maintenant vérifier la stabilité des hôtes de ce groupe de migration !!\n"
    preactionstep4 = "!! IMPORTANT !! Veuillez confirmer que les hôtes de ce groupe de migration sont stables."
    prepairsplitR = "pairsplit -R ( groupe à SMPL )"
    hurpreactionstep3 = "La relation de paire HUR DR héritée sera supprimée (pairsplit -S) afin que la division de la migration GAD se produise.\n\nVeuillez confirmer que vous pouvez continuer."

class Japanese:
    step2 = "次のステップを実行する前に、次のアクションを完了してください:\n\n\t1) ホストへのターゲットアレイパスの存在（ゾーン）\n\t2) ホスト上の新しいパスをスキャンします\m\t3) ソース配列パスを無効にして削除します（該当する場合）\n\t4) ホストへのソース配列パスのゾーン解除"
    acknowledgenextstep = "上記のアクションを理解し、次のステップに進む前に必要なタスクをすべて完了することを確認してください"
    preactionstep3 = "この手順を続行する前に、次のアクションを完了してください:\n\n\t1) この移行グループでは、ホストがターゲットパスを使用していることを確認します\n\t2) この移行グループでは、ホストがソースパスにアクセスできないことを確認します"
    prepairsplitRS = "pairsplit -RS（SWSグループからSSWSへ）"
    endofpairsplitRS = "!!重要!!この移行グループのホストの安定性を確認してください!!"
    preactionstep4 = "!!重要!!この移行グループのホストが安定していることを確認してください。"
    prepairsplitR = "pairsplit -R（SMPLにグループ化）"
    hurpreactionstep3 = "GADの移行を分割するために、レガシーHUR DRペアの関係は削除されます（pairsplit -S）。\n\n続行してもよいことを確認してください。"

class Gadmessaging:
    def __init__(self,language,log=''):
        self.langchoices = { 'english': English(), 'french': French(), 'japanese': Japanese() }
        self.language = language
        self.log = log
        self.initlang()

    def initlang(self):
        try:
            self.lang = self.langchoices[self.language]
        except KeyError as e:
            self.log.info('Unsupported language {}, default to English'.format(self.language))
            self.lang = self.langchoices['english']

    def message(self,key):
        return getattr(self.lang,key)



    

