package nl.esciencecenter.newsreader.hbase;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SizerFunction  extends BaseOperation implements Function {
    public SizerFunction(Fields fieldDeclaration ) {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry argument = functionCall.getArguments();
        String docName = argument.getString(0);
        String docContent = argument.getString(1);
        if (docContent == null) {
            docContent = "";
        }
        Tuple result = new Tuple();
        result.add(docName);
        result.add(docContent.length());
        functionCall.getOutputCollector().add(result);
    }
}
