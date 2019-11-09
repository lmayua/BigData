package ext.bigdata.kafka.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

    /**
     * 解析avsc格式文件，将文件防止common的resource目录中
     *
     * @param fileNm
     * @return
     * @throws Exception
     */
    public static Schema getSchema(String fileNm) {
        InputStream inputStream = null;
        BufferedReader bufferedReader = null;
        StringBuilder avroStr = new StringBuilder();
        try {
            inputStream = SchemaUtils.class.getResourceAsStream("/" + fileNm);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String tempStr;
            while ((tempStr = bufferedReader.readLine()) != null) {
                avroStr.append(tempStr);
            }
            return new Schema.Parser().parse(avroStr.toString());
        } catch (Exception e) {
            // TODO 
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
            //throw new BrockRuntimeException("SchemaUtils get avsc file failed", e);
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.error("SchemaUtils InputStream close failed :", e);
                }
            }
            if (null != bufferedReader) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.error("SchemaUtils BufferedReader close failed :", e);
                }
            }
        }
    }

    /**
     * 封装schema 成byte[]
     *
     * @param outObject
     * @param schema
     * @return
     * @throws Exception
     */
    public static byte[] getSchemaByte(GenericRecord outObject, Schema schema) {
        ByteArrayOutputStream out = null;
        try {
            DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
            out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(outObject, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            // TODO 
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
            //throw new BrockRuntimeException("SchemaUtils get avsc file failed", e);
        } finally {
            if (null != out) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.error("SchemaUtils ByteArrayOutputStream close failed :", e);
                }
            }
        }
    }

}
