package io.univalence.parka

import com.twitter.algebird.QTree
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{ Decoder, Encoder }
import io.circe.generic.auto._

// For more information about decoding and encoding with circe: https://circe.github.io/circe/codec.html

case class KeyVal[K, V](key: K, value: V)

object Serde extends ParkaEncodersAndDecoders {
  def toJson(pa: ParkaAnalysis): Json = pa.asJson

  object subPart {
    def toJson(x: Histogram): Json = x.asJson
  }

  def fromJson(json: Json): Result[ParkaAnalysis] = json.as[ParkaAnalysis]
}

trait ParkaEncodersAndDecoders {

  case class QTU(offset: Long,
                 level: Int,
                 count: Long,
                 lowerChild: Option[QTree[Unit]],
                 upperChild: Option[QTree[Unit]])

  // ENCODERS

  implicit val encoderDoubleKey: KeyEncoder[Double]     = KeyEncoder[String].contramap(_.toString)
  implicit val encoderLongKey: KeyEncoder[Long]         = KeyEncoder[String].contramap(_.toString)
  implicit val encoderMapDL: Encoder[Map[Double, Long]] = Encoder.encodeMap[Double, Long]
  implicit val encoderMapLL: Encoder[Map[Long, Long]]   = Encoder.encodeMap[Long, Long]

  //implicit lazy val encoderLargeHistogram: Encoder[LargeHistogram]  = deriveEncoder
  //implicit lazy val encoderSmallHistogram: Encoder[SmallHistogramD] = deriveEncoder
  implicit def encoderHistogram: Encoder[Histogram] =
    Encoder.instance {
      case x if x.count == 0  => Json.Null
      case l: LargeHistogram  => l.asJson
      case l: SmallHistogramD => l.asJson
      case l: SmallHistogramL => l.asJson
    }
  implicit def encoderQTree: Encoder[QTree[Unit]] = deriveEncoder[QTU].contramap(x => QTU(x._1, x._2, x._3, x._5, x._6))

  implicit val encoderDelta: Encoder[Delta]                 = deriveEncoder
  implicit val encoderMapDelta: Encoder[Map[String, Delta]] = Encoder.encodeMap[String, Delta]
  implicit val encodeKeySeqString: KeyEncoder[Set[String]]  = KeyEncoder.instance[Set[String]](_.mkString("/"))

  implicit val encoderMap: Encoder[Map[Set[String], Long]] = Encoder
    .encodeSeq[KeyVal[Set[String], Long]]
    .contramap(x => x.map({ case (k, v) => KeyVal(k, v) }).toSeq)

  implicit val encoderParkaAnalysis: Encoder[ParkaAnalysis] = deriveEncoder

  // DECODERS

  implicit val decoderDoubleKey: KeyDecoder[Double]     = KeyDecoder[String].map(_.toDouble)
  implicit val decoderLongKey: KeyDecoder[Long]         = KeyDecoder[String].map(_.toLong)
  implicit val decoderMapDL: Decoder[Map[Double, Long]] = Decoder.decodeMap[Double, Long]
  implicit val decoderMapLL: Decoder[Map[Long, Long]]   = Decoder.decodeMap[Long, Long]

  //implicit def decoderLargeHistogram: Decoder[LargeHistogram]   = deriveDecoder
  //implicit def decoderSmallHistogram: Decoder[SmallHistogramD]  = deriveDecoder[SmallHistogramD]
  //implicit def decoderSmallHistogramL: Decoder[SmallHistogramL] = deriveDecoder[SmallHistogramL]
  import cats.syntax.functor._
  implicit def decoderHistogram: Decoder[Histogram] =
    Decoder[LargeHistogram].widen or
      Decoder[SmallHistogramL].widen[Histogram] or
      Decoder[SmallHistogramD].widen[Histogram] or
      Decoder.decodeNone.map(_ => Histogram.empty)
  implicit def decoderQTree: Decoder[QTree[Unit]] =
    deriveDecoder[QTU].map(x => new QTree[Unit](x.offset, x.level, x.count, {}, x.lowerChild, x.upperChild))

  implicit val decoderDescribe: Decoder[Describe]           = deriveDecoder
  implicit val decoderDelta: Decoder[Delta]                 = deriveDecoder
  implicit val decoderMapDelta: Decoder[Map[String, Delta]] = Decoder.decodeMap[String, Delta]
  implicit val decodeKeySeqString: KeyDecoder[Set[String]] = KeyDecoder.instance[Set[String]]({
    case "" => Some(Set.empty)
    case x  => Some(x.split('/').toSet)
  })

  implicit val decoderMap: Decoder[Map[Set[String], Long]] =
    Decoder.decodeSeq[KeyVal[Set[String], Long]].map(x => x.map(x => x.key -> x.value).toMap)
}
