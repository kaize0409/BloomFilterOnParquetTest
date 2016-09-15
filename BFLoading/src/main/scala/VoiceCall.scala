/**
  * Created by kaiser on 16/8/31.
  */
case class VoiceCall(
                      cs_refid: String,
                      ngn_refid: String,
                      starttime: BigInt,
                      millisec: Int,
                      service_type: Int,
                      callerno: BigInt,
                      callertype: Int,
                      callerimsi: String,
                      calledno: BigInt,
                      calledtype: Int,
                      calledimsi: String,
                      alert_time: BigInt,
                      answer_time: BigInt,
                      disconn_time: BigInt,
                      end_time: BigInt,
                      causelst: String,
                      protocols: String,
                      status: Int,
                      reserved1: String,
                      reserved2: String,
                      reserved3: String,
                      reserved4: String,
                      signalstorage: String,
                      cldnoa: Int,
                      cplnoa: Int,
                      firfailprot: BigInt,
                      firfailmsg: Int,
                      firfailcause: BigInt,
                      callid: String,
                      callduration: BigInt,
                      rel_time: BigInt,
                      rlc_time: BigInt,
                      layer1id: Int,
                      layer2id: Int,
                      layer3id: Int,
                      layer4id: Int,
                      layer5id: Int,
                      layer6id: Int,
                      formatcallerno: BigInt,
                      formatcallertype: Int,
                      formatcalledno: BigInt,
                      formatcalledtype: Int,
                      orgcalledno: BigInt,
                      orgcalledtype: Int,
                      orgcallednoa: Int,
                      csfbind: Int,
                      failtype: String,
                      firfailpd: Int,
                      firfailside: Int,
                      callerrelcgi: String,
                      calledrelcgi: String,
                      ni: Int,
                      opc: Int,
                      srcip: BigInt,
                      poolid: BigInt,
                      reserved5: String,
                      reserved6: String,
                      last_msisdn: String
                    ) {
  def this(attributes: Array[String]) = {

    this(attributes(0), attributes(1), TransferUtil.bigIntTransfer(attributes(2).trim), TransferUtil.intTransfer(attributes(3).trim),
      TransferUtil.intTransfer(attributes(4).trim), TransferUtil.bigIntTransfer(attributes(5).trim), TransferUtil.intTransfer(attributes(6).trim),
      attributes(7), TransferUtil.bigIntTransfer(attributes(8).trim), TransferUtil.intTransfer(attributes(9).trim), attributes(10),
      TransferUtil.bigIntTransfer(attributes(11).trim), TransferUtil.bigIntTransfer(attributes(12).trim), TransferUtil.bigIntTransfer(attributes(13).trim),
      TransferUtil.bigIntTransfer(attributes(14).trim), attributes(15), attributes(16), TransferUtil.intTransfer(attributes(17).trim),
      attributes(18), attributes(19), attributes(20), attributes(21), attributes(22), TransferUtil.intTransfer(attributes(23).trim),
      TransferUtil.intTransfer(attributes(24).trim), TransferUtil.bigIntTransfer(attributes(25).trim), TransferUtil.intTransfer(attributes(26).trim),
      TransferUtil.bigIntTransfer(attributes(27).trim), attributes(28), TransferUtil.bigIntTransfer(attributes(29).trim), TransferUtil.bigIntTransfer(attributes(30).trim),
      TransferUtil.bigIntTransfer(attributes(31).trim), TransferUtil.intTransfer(attributes(32).trim), TransferUtil.intTransfer(attributes(33).trim),
      TransferUtil.intTransfer(attributes(34).trim), TransferUtil.intTransfer(attributes(35).trim), TransferUtil.intTransfer(attributes(36).trim),
      TransferUtil.intTransfer(attributes(37).trim), TransferUtil.bigIntTransfer(attributes(38).trim), TransferUtil.intTransfer(attributes(39).trim),
      TransferUtil.bigIntTransfer(attributes(40).trim), TransferUtil.intTransfer(attributes(41).trim), TransferUtil.bigIntTransfer(attributes(42).trim),
      TransferUtil.intTransfer(attributes(43).trim), TransferUtil.intTransfer(attributes(44).trim), TransferUtil.intTransfer(attributes(45).trim), attributes(46),
      TransferUtil.intTransfer(attributes(47).trim), TransferUtil.intTransfer(attributes(48).trim), attributes(49), attributes(50),
      TransferUtil.intTransfer(attributes(51).trim), TransferUtil.intTransfer(attributes(52).trim), TransferUtil.bigIntTransfer(attributes(53).trim),
      TransferUtil.bigIntTransfer(attributes(54).trim), attributes(55), attributes(56), TransferUtil.patitionKeyTransfer(attributes(8))
    )
  }

}