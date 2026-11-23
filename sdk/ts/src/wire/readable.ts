// What lets the wire grow (spec/README.md, "Growing the wire"): a list
// whose reader keeps the entries it can decode and skips the rest.
// Encoding stays strict.
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

export const ReadableArray = <S extends Schema.Codec<unknown, unknown>>(
  item: S,
) => {
  const decode = Schema.decodeUnknownOption(item);
  const encode = Schema.encodeSync(item);
  return Schema.Array(Schema.Unknown).pipe(
    Schema.decodeTo(
      Schema.Array(Schema.toType(item)),
      SchemaTransformation.transform<
        ReadonlyArray<S["Type"]>,
        ReadonlyArray<unknown>
      >({
        decode: (entries) =>
          entries.flatMap((entry) => Option.toArray(decode(entry))),
        encode: (values) => values.map((value) => encode(value)),
      }),
    ),
  );
};
