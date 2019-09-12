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

  implicit val encoderDoubleKey: KeyEncoder[Double] = KeyEncoder[String].contramap(_.toString)
  implicit val decoderDoubleKey: KeyDecoder[Double] = KeyDecoder[String].map(_.toDouble)

  implicit val encoderLongKey: KeyEncoder[Long] = KeyEncoder[String].contramap(_.toString)
  implicit val decoderLongKey: KeyDecoder[Long] = KeyDecoder[String].map(_.toLong)

  implicit val encoderQTree: Encoder[QTree[Unit]] = deriveEncoder[QTU].contramap(x => QTU(x._1, x._2, x._3, x._5, x._6))
  implicit val decoderQTree: Decoder[QTree[Unit]] =
    deriveDecoder[QTU].map(x => new QTree[Unit](x.offset, x.level, x.count, {}, x.lowerChild, x.upperChild))

  implicit val encoderHistogram: Encoder[Histogram] =
    Encoder.instance {
      case x if x.count == 0  => Json.Null
      case l: LargeHistogram  => l.asJson
      case l: SmallHistogramD => l.asJson
      case l: SmallHistogramL => l.asJson
    }
  implicit val decoderHistogram: Decoder[Histogram] = {
    import cats.syntax.functor._
    Decoder[LargeHistogram].widen or
      Decoder[SmallHistogramL].widen[Histogram] or
      Decoder[SmallHistogramD].widen[Histogram] or
      Decoder.decodeNone.map(_ => Histogram.empty)
  }

  implicit val encoderEnumKey: Encoder[EnumKey] = {
    Encoder.instance {
      case StringEnumKey(str)   => str.asJson
      case BothStringEnumKey(b) => b.asJson
    }
  }

  implicit val decoderEnumKey: Decoder[EnumKey] = {
    Decoder[String].map(StringEnumKey.apply) or
      Decoder[Both[String]].map(BothStringEnumKey.apply)
  }

  implicit val encoderEnumMap: Encoder[Map[EnumKey, Long]] = {
    Encoder.instance(_.toSeq.asJson)
  }

  implicit val decoderEnumMap: Decoder[Map[EnumKey, Long]] = {
    Decoder[Seq[(EnumKey, Long)]].map(_.toMap)
  }

  implicit val encoderStringEnum: Encoder[Enum] = {
    Encoder.instance {
      case x: SmallEnum => x.asJson
      case x: LargeEnum => SmallEnum(x.heavyHitters).asJson
    }
  }

  implicit val decodeStringEnum: Decoder[Enum] = {
    import cats.syntax.functor._
    Decoder[SmallEnum].widen
  }

  implicit val encoderDescribe: Encoder[Describe] = Encoder.instance({
    case x if x.count == 0 => Json.Null
    case x                 => deriveEncoder[Describe].apply(x)
  })
  implicit val decoderDescribe: Decoder[Describe] = deriveDecoder[Describe] or Decoder.decodeNone.map(
    _ => Describe.empty
  )

  implicit val encoderDelta: Encoder[Delta] = deriveEncoder
  implicit val decoderDelta: Decoder[Delta] = deriveDecoder

  implicit val encoderMap: Encoder[Map[Set[String], DeltaByRow]] = Encoder
    .encodeSeq[KeyVal[Set[String], DeltaByRow]]
    .contramap(x => x.map({ case (k, v) => KeyVal(k, v) }).toSeq)
  implicit val decoderMap: Decoder[Map[Set[String], DeltaByRow]] =
    Decoder.decodeSeq[KeyVal[Set[String], DeltaByRow]].map(x => x.map(x => x.key -> x.value).toMap)

  implicit val encoderInner: Encoder[Inner] = {
    val encodeInner = deriveEncoder[Inner]
    val encodeMap   = Encoder.encodeMap[String, Delta]
    Encoder.instance(inner => {
      encodeInner(inner).mapObject(jobj => jobj.add("byColumn", encodeMap(inner.byColumn)))
    })
  }

  implicit val encoderParkaAnalysis: Encoder[ParkaAnalysis] = deriveEncoder
  implicit val decoderParkaAnalysis: Decoder[ParkaAnalysis] = deriveDecoder
}
