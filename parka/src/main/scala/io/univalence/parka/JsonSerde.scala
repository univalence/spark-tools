package io.univalence.parka

import com.twitter.algebird.QTree
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{ Decoder, Encoder }
import io.circe.generic.auto._
/*import io.univalence.parka.Delta.{
  DeltaBoolean,
  DeltaCombine,
  DeltaDate,
  DeltaDouble,
  DeltaLong,
  DeltaString,
  DeltaTimestamp
}
 */
case class KeyVal[K, V](key: K, value: V)

trait ParkaDecoder {
  type QTU = (Long, Int, Long, Option[QTree[Unit]], Option[QTree[Unit]])

  // Encoder
  implicit def encoderQTree: Encoder[QTree[Unit]] =
    deriveEncoder[QTU].contramap(x => (x._1, x._2, x._3, x._5, x._6))
  implicit val encoderDelta: Encoder[Delta]                 = deriveEncoder
  implicit val encoderMapDelta: Encoder[Map[String, Delta]] = Encoder.encodeMap[String, Delta]
  implicit val encodeKeySeqString: KeyEncoder[Seq[String]] = new KeyEncoder[Seq[String]] {
    override def apply(s: Seq[String]): String = s.mkString("/")
  }
  implicit val encoderMap: Encoder[Map[Seq[String], Long]] = Encoder
    .encodeSeq[KeyVal[Seq[String], Long]]
    .contramap(x => x.map({ case (k, v) => KeyVal(k, v) }).toSeq)

  implicit val encoderParkaAnalysis: Encoder[ParkaAnalysis] = deriveEncoder
  /*implicit val encoderDeltaLong: Encoder[DeltaLong]           = deriveEncoder
  implicit val encoderDeltaBoolean: Encoder[DeltaBoolean]     = deriveEncoder
  implicit val encoderDeltaString: Encoder[DeltaString]       = deriveEncoder
  implicit val encoderDeltaDouble: Encoder[DeltaDouble]       = deriveEncoder
  implicit val encoderDeltaDate: Encoder[DeltaDate]           = deriveEncoder
  implicit val encoderDeltaTimestamp: Encoder[DeltaTimestamp] = deriveEncoder
  implicit val encoderDeltaCombine: Encoder[DeltaCombine]     = deriveEncoder*/

  // Decoder
  implicit def decoderQTree: Decoder[QTree[Unit]] =
    deriveDecoder[QTU].map(x => new QTree[Unit](x._1, x._2, x._3, {}, x._4, x._5))
  implicit val decoderDescribe: Decoder[Describe]           = deriveDecoder
  implicit val decoderDelta: Decoder[Delta]                 = deriveDecoder
  implicit val decoderMapDelta: Decoder[Map[String, Delta]] = Decoder.decodeMap[String, Delta]
  implicit val decodeKeySeqString: KeyDecoder[Seq[String]] = new KeyDecoder[Seq[String]] {
    override def apply(s: String): Option[Seq[String]] = Some(if (s == "") Nil else s.split("/"))
  }
  implicit val decoderMap: Decoder[Map[Seq[String], Long]] =
    Decoder.decodeSeq[KeyVal[Seq[String], Long]].map(x => x.map(x => x.key -> x.value).toMap)

  /*implicit val decoderParkaAnalysis: Decoder[ParkaAnalysis]   = deriveDecoder
  implicit val decoderDeltaLong: Decoder[DeltaLong]           = deriveDecoder
  implicit val decoderDeltaBoolean: Decoder[DeltaBoolean]     = deriveDecoder
  implicit val decoderDeltaString: Decoder[DeltaString]       = deriveDecoder
  implicit val decoderDeltaDouble: Decoder[DeltaDouble]       = deriveDecoder
  implicit val decoderDeltaDate: Decoder[DeltaDate]           = deriveDecoder
  implicit val decoderDeltaTimestamp: Decoder[DeltaTimestamp] = deriveDecoder
  implicit val decoderDeltaCombine: Decoder[DeltaCombine]     = deriveDecoder*/
}

object ParkaAnalysisSerde extends ParkaDecoder {
  def toJson(pa: ParkaAnalysis): Json             = pa.asJson
  def fromJson(json: Json): Result[ParkaAnalysis] = json.as[ParkaAnalysis]
}
