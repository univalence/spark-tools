package io.univalence.parka

import com.twitter.algebird.QTree
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{ Decoder, Encoder }
import io.circe.generic.auto._
case class KeyVal[K, V](key: K, value: V)

object Serde extends ParkaDecoder {
  def toJson(pa: ParkaAnalysis): Json             = pa.asJson
  def fromJson(json: Json): Result[ParkaAnalysis] = json.as[ParkaAnalysis]
}

trait ParkaDecoder {
  case class QTU(offset: Long,
                 level: Int,
                 count: Long,
                 lowerChild: Option[QTree[Unit]],
                 upperChild: Option[QTree[Unit]])

  // Encoder
  implicit def encoderQTree: Encoder[QTree[Unit]]           = deriveEncoder[QTU].contramap(x => QTU(x._1, x._2, x._3, x._5, x._6))
  implicit val encoderDelta: Encoder[Delta]                 = deriveEncoder
  implicit val encoderMapDelta: Encoder[Map[String, Delta]] = Encoder.encodeMap[String, Delta]
  implicit val encodeKeySeqString: KeyEncoder[Seq[String]]  = KeyEncoder.instance[Seq[String]](_.mkString("/"))

  implicit val encoderMap: Encoder[Map[Seq[String], Long]] = Encoder
    .encodeSeq[KeyVal[Seq[String], Long]]
    .contramap(x => x.map({ case (k, v) => KeyVal(k, v) }).toSeq)

  implicit val encoderParkaAnalysis: Encoder[ParkaAnalysis] = deriveEncoder

  // Decoder
  implicit def decoderQTree: Decoder[QTree[Unit]] =
    deriveDecoder[QTU].map(x => new QTree[Unit](x.offset, x.level, x.count, {}, x.lowerChild, x.upperChild))
  implicit val decoderDescribe: Decoder[Describe]           = deriveDecoder
  implicit val decoderDelta: Decoder[Delta]                 = deriveDecoder
  implicit val decoderMapDelta: Decoder[Map[String, Delta]] = Decoder.decodeMap[String, Delta]
  implicit val decodeKeySeqString: KeyDecoder[Seq[String]] = KeyDecoder.instance[Seq[String]]({
    case "" => Some(Nil)
    case x  => Some(x.split('/'))
  })

  implicit val decoderMap: Decoder[Map[Seq[String], Long]] =
    Decoder.decodeSeq[KeyVal[Seq[String], Long]].map(x => x.map(x => x.key -> x.value).toMap)
}
