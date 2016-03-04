package kafka.examples.kryo.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import de.javakaffee.kryoserializers.*;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.*;

public class KryoUtils {

    private static final Logger logger = LoggerFactory.getLogger(KryoUtils.class);
    static final List<String> SINGLETON_LIST = Collections.singletonList("");
    static final Set<String> SINGLETON_SET = Collections.singleton("");
    static final Map<String, String> SINGLETON_MAP = Collections.singletonMap("", "");
    static final Set<String> SET_FROM_MAP = Collections.newSetFromMap(new HashMap<String, Boolean>());
    private static final KryoFactory factory = new KryoFactory() {

        public Kryo create() {
            Kryo kryo = new Kryo();
            try {
                kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
                kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
                kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
                kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
                kryo.register(SINGLETON_LIST.getClass(), new CollectionsSingletonListSerializer());
                kryo.register(SINGLETON_SET.getClass(), new CollectionsSingletonSetSerializer());
                kryo.register(SINGLETON_MAP.getClass(), new CollectionsSingletonMapSerializer());
                kryo.setRegistrationRequired(false);

                UnmodifiableCollectionsSerializer.registerSerializers(kryo);
                SynchronizedCollectionsSerializer.registerSerializers(kryo);
                ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            } catch (Exception e) {
                logger.error("Exception occurred", e);
            }
            return kryo;
        }
    };

    private static final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();


    public static byte[] serialize(final Object obj) {

        return pool.run(new KryoCallback<byte[]>() {

            @Override
            public byte[] execute(Kryo kryo) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                Output output = new Output(stream);
                kryo.writeClassAndObject(output, obj);
                output.close();
                return stream.toByteArray();
            }

        });
    }

    @SuppressWarnings("unchecked")
    public static <V> V deserialize(final byte[] objectData) {

        return (V) pool.run(new KryoCallback<V>() {

            @Override
            public V execute(Kryo kryo) {
                Input input = new Input(objectData);
                return (V) kryo.readClassAndObject(input);
            }

        });
    }

    public static <V> V deepCopy(final V obj) {

        return (V) pool.run(new KryoCallback<V>() {

            @Override
            public V execute(Kryo kryo) {
                return (V) kryo.copy(obj);
            }

        });
    }

}