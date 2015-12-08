package fr.finaxys.tutorials.utils.spark.streaming;


import fr.finaxys.tutorials.utils.spark.models.DataRow;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Created by finaxys on 12/4/15.
 */
public class EmptyReceiver extends Receiver<DataRow> {

    private static final long serialVersionUID = 8377332385604261854L;


    public EmptyReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
    }


    @Override
    public void onStop() {
    }

}
