import { VulcanExtensionId } from "@vulcan-sql/core";
import { BaseResponseFormatter, KoaContext } from "@vulcan-sql/serve";
import * as Stream from "stream";

class MvtTransformer extends Stream.Transform {
  constructor({
    options,
  }: {
    options?: Stream.TransformOptions;
  } = {}) {
    options = options || {
      writableObjectMode: true,
      readableObjectMode: false,
    };

    super(options);
  }

  public override _transform(
    chunk: any,
    _encoding: BufferEncoding,
    callback: Stream.TransformCallback
  ) {
    // TODO: let column name configurable
    this.push(chunk.mvt);
    callback();
  }
}

@VulcanExtensionId("mvt")
export class MvtFormatter extends BaseResponseFormatter {
  public format(data: Stream.Readable) {
    const transformer = new MvtTransformer();
    data.pipe(transformer);
    return transformer;
  }

  public toResponse(
    stream: Stream.Readable | Stream.Transform,
    ctx: KoaContext
  ) {
    ctx.response.body = stream;
    ctx.response.set("Content-type", "application/octet-stream");
  }
}
