package cargomatcher;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class CargoToTransporterMatchReporterPoC {

    public static void main(String args[]){

        if(args.length < 3 ||
                args[0] == null || args[0].length() == 0 ||
                args[1] == null || args[1].length() == 0 ||
                args[2] == null || args[2].length() == 0
                ){
            System.out.println("Missing resource location parameter");
        }

        //15 minutes time difference between GPS signals
        final long TIME_DIFFERENCE = 15 * 60 * 1000;
        final String OUTPUT_DIR = args[2];
        final String TRANSPORTERS_FILE = args[0];
        final String CARGO_FILE = args[1];
        final int[] transportersCoordinatesIndexes = {2,3};
        final int[] cargoCoordinatesIndexes = {14,15};

        List<Map<String, String[]>> transportersMapsList = null;
        List<Map<String, String[]>> cargoMapsList = null;
        List<String> cargoHeaderList = null;

        try {
            //read data from manually converted excel files
            transportersMapsList = readFile(TRANSPORTERS_FILE, transportersCoordinatesIndexes, "");
            cargoMapsList = readFile(CARGO_FILE, cargoCoordinatesIndexes,"/ Valid");

            //get header
            File inputF = new File(CARGO_FILE);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            // skip the header of the csv
            Optional<String> cargoHeader = br.lines().findFirst();
            cargoHeaderList = new ArrayList<String>(Arrays.asList(cargoHeader.get().split("\t")));
        }catch (IOException ioe){
            ioe.printStackTrace();
            System.exit(0);
        }

        //group transporters list of maps by unique key '<Longitude>*<Latitude>'
        Map<String, List<String[]>> uniquetransportersMap = new HashMap<String, List<String[]>>();
        for(Map<String, String[]> map : transportersMapsList){
            Set<Map.Entry<String, String[]>> mapEntrySet =  map.entrySet();
            //there is only 1 entry in each map
            Map.Entry<String, String[]> mapEntry = mapEntrySet.iterator().next();
            String coordKey = mapEntry.getKey();
            String[] attributes = mapEntry.getValue();
            List<String[]> uniquetransportersCoordinatesList = uniquetransportersMap.get(coordKey);
            if(uniquetransportersCoordinatesList == null){
                uniquetransportersCoordinatesList = new ArrayList<String[]>();
            }
            uniquetransportersCoordinatesList.add(attributes);
            uniquetransportersMap.put(coordKey, uniquetransportersCoordinatesList);
        }

        //iterate over cargo list and find matching by coordinates transporters map
        SimpleDateFormat transporterDateFormatter = new SimpleDateFormat("MM/dd/yy HH:mm");
        SimpleDateFormat cargoDateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        Date cargoDate = null;
        List<String[]> matchedcargo = new ArrayList<String[]>();
        for(Map<String, String[]> cargoMap : cargoMapsList){
            Set<Map.Entry<String, String[]>> mapEntrySet =  cargoMap.entrySet();
            //there is only 1 entry in each map
            Map.Entry<String, String[]> mapEntry = mapEntrySet.iterator().next();
            String coordKey = mapEntry.getKey();
            String[] cargoAttributes = mapEntry.getValue();

            try {
                cargoDate = cargoDateFormatter.parse(cargoAttributes[0]);
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println("Inconsistent date format");
            }

            //match cargo signal date with transporter signal date
            List<String[]> transportersAttributesArrayList = (List<String[]>) uniquetransportersMap.get(coordKey);
            if(transportersAttributesArrayList != null && transportersAttributesArrayList.size() > 0){
                boolean isCargoFound = false;
                //inspect all transporter coordinates entries
                for(String[] transportersAttributesArray : transportersAttributesArrayList){
                    Date transporterSignalDate = null;
                    try {
                        transporterSignalDate = transporterDateFormatter.parse(transportersAttributesArray[1]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if (Math.abs(transporterSignalDate.getTime() - cargoDate.getTime()) < TIME_DIFFERENCE ){
                        cargoAttributes[cargoAttributes.length - 1] = transportersAttributesArray[0];
                        isCargoFound = true;
                        break;
                    }
                }
                if(isCargoFound){
                    matchedcargo.add(cargoAttributes);
                    continue;
                }
            }
            matchedcargo.add(cargoAttributes);
        }

        //SAVE MATCHED ENTRIES
        cargoHeaderList.add("transporter (MMSI)");
        String[] headersArr = new String[cargoHeaderList.size()];
        headersArr = cargoHeaderList.toArray(headersArr);
        List<String[]> listToWrite = new ArrayList<String[]>();
        listToWrite.add(headersArr);
        listToWrite.addAll(matchedcargo);

        // writer
        FileWriter writer = null;
        try {
            File outputDir = new File(OUTPUT_DIR);
            if (! outputDir.exists()){
                outputDir.mkdirs();
            }
            writer = new FileWriter(OUTPUT_DIR + "\\match.txt");
            writer.flush();
            // data
            for(String[] array: listToWrite) {
                String appender = "";
                for(String str : array){
                    writer.write(appender + str);
                    appender = "\t";
                }
                writer.write("\n");
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String[]>> readFile(final String filePath, int[] indexes, String filter) throws IOException{
        List<Map<String, String[]>> mapsList = new ArrayList<Map<String, String[]>>();
        try{
            File inputF = new File(filePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            // skip the header of the csv
            mapsList = br.lines()
                    .skip(1)
                    .filter(line -> line.contains(filter))
                    .map(line -> line.split("\t"))
                    .map(array -> {
                        Map<String, String[]> map = new HashMap<String, String[]>();
                        String coordKey = array[indexes[0]].concat("*").concat(array[indexes[1]]);
                        ArrayList<String> extraElementList = new ArrayList<String>(Arrays.asList(array));
                        extraElementList.add("");
                        map.put(coordKey, (String[])extraElementList.toArray(new String[0]));
                        return map;
                    })
                    .collect(Collectors.toCollection(ArrayList::new));
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mapsList;
    }
}
