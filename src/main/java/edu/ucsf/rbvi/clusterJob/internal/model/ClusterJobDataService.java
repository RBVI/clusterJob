package edu.ucsf.rbvi.clusterJob.internal.model;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.SUIDUtil;
import org.cytoscape.model.CyEdge;
import org.cytoscape.model.CyIdentifiable;
import org.cytoscape.model.CyNetwork;
import org.cytoscape.model.CyNetworkFactory;
import org.cytoscape.model.CyNetworkManager;
import org.cytoscape.model.CyNode;
import org.cytoscape.model.CyRow;
import org.cytoscape.model.CyTable;
import org.cytoscape.model.subnetwork.CyRootNetwork;
import org.cytoscape.service.util.CyServiceRegistrar;

public class ClusterJobDataService implements CyJobDataService {
	final private CyServiceRegistrar registrar;
	final private CyNetworkManager networkManager;
	final private CyNetworkFactory networkFactory;

	public ClusterJobDataService(CyServiceRegistrar registrar) {
		this.registrar = registrar;
		this.networkManager = registrar.getService(CyNetworkManager.class);
		this.networkFactory = registrar.getService(CyNetworkFactory.class);
	}

	@Override
	public String getServiceName() { return "ClusterJobDataService"; }

	@Override
	public CyJobData addData(CyJobData data, String key, Object value) {
		if (data == null)
			data = new ClusterJobData();
		data.put(key, data);
		return data;
	}

	@Override
	public CyJobData addData(CyJobData data, String key, Map<Object, Object> map) {
		return addData(data, key, (Object)map);
	}

	@Override
	public CyJobData addData(CyJobData data, String key, CyNetwork network, 
	                         List<? extends CyIdentifiable> nodesAndEdges,
	                         List<String> nodeColumns,
	                         List<String> edgeColumns) {
		if (data == null)
			data = new ClusterJobData();
		Map<String, Object> netMap = new HashMap<>();
		addCyIdentifiable(netMap, network, network);
		List<Map<String, Object>> nodeList = new ArrayList<>();
		List<Map<String, Object>> edgeList = new ArrayList<>();
		for (CyIdentifiable id: nodesAndEdges) {
			if (id instanceof CyNode) {
				Map<String, Object> node = makeNodeMap(network, (CyNode)id, nodeColumns);
				nodeList.add(node);
			} else if (id instanceof CyEdge) {
				Map<String, Object> edge = makeEdgeMap(network, (CyEdge)id, edgeColumns);
				edgeList.add(edge);
			}
		}
		if (nodeList.size() > 0)
			netMap.put("nodes", nodeList);
		if (edgeList.size() > 0)
			netMap.put("edges", edgeList);
		data.put(key, netMap);
		return data;
	}

	@Override
	public CyJobData getDataInstance() {
		return new ClusterJobData();
	}

	@Override
	public Object getData(CyJobData data, String key) {
		if (!data.containsKey(key))
			return null;
		return data.get(key);
	}

	@Override
	public Map<Object, Object> getMapData(CyJobData data, String key) {
		if (!data.containsKey(key))
			return null;
		Object obj = data.get(key);
		if (obj == null) return null;
		if (obj instanceof Map)
			return (Map<Object, Object>) obj;
		return null;
	}

	@Override
	public CyNetwork getNetworkData(CyJobData data, String key) {
		if (!data.containsKey(key)) {
			return null;
		}
		Object obj = data.get(key);
		if (obj == null) return null;
		// Our network should actually be a map
		if (!(obj instanceof Map)) {
			return null;
		}

		Map<String, Object> netMap = (Map<String, Object>)obj;
		Long networkSUID = (Long)netMap.get("id");
		String networkName = (String)netMap.get("name");

		CyJob job = null;
		CyNetwork network = null;
		Map<Long, CyIdentifiable> suidMap = null;

		// Do we have a job saved?
		if (data.containsKey("job")) {
			job = (CyJob)data.get("job");

			// See if this network was saved in the session information
			network = SUIDUtil.restoreNetwork(job, networkManager, networkSUID);

			List<Long> oldIds = new ArrayList<>();
			if (network != null) {
				if (netMap.containsKey("nodes")) {
					for (Object o: (JSONArray)netMap.get("nodes")) {
						Long id = (Long)((JSONObject)obj).get("id");
						if (id != null) oldIds.add(id);
					}
				}
				if (netMap.containsKey("edges")) {
					for (Object o: (JSONArray)netMap.get("edges")) {
						Long id = (Long)((JSONObject)obj).get("id");
						if (id != null) oldIds.add(id);
					}
				}
			}
			suidMap = SUIDUtil.restoreSUIDs(job, network, oldIds);
		}

		// Find the network.  If we can't find one, create one
		if (network == null)
			network = findNetwork(networkSUID, networkName);
	
		Map<String, CyNode> nodeNameMap = null;
		if (netMap.containsKey("nodes")) {
			nodeNameMap = getNodes(network, (JSONArray)netMap.get("nodes"), suidMap);
		}
		if (netMap.containsKey("edges")) {
			getEdges(network, nodeNameMap, (JSONArray)netMap.get("edges"), suidMap);
		}
		return network;
	}

	public Object getSerializedData(CyJobData data) {
		// Convert the data into JSON
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		for (String key: data.keySet()) {
			Object obj = data.get(key);
			sb.append(quote(key)+": ");
			convertToJSON(obj, sb);
			sb.append(",\n");
		}
		sb.append("}");
		return sb.toString();
	}

	@Override
	public CyJobData unSerialize(InputStream inputStream) {
		return unSerialize(new BufferedReader(new InputStreamReader(inputStream)));
	}

	@Override
	public CyJobData unSerialize(Reader reader) {
		JSONObject json;
		JSONParser parser = new JSONParser();
		try {
			json = (JSONObject) parser.parse(reader);
		} catch (IOException io) {
			return null;
		} catch (ParseException pe) {
			return null;
		}
		return convertJSONToCyData(json);
	}

	@Override
	public CyJobData unSerialize(Object object) {
		JSONObject json;
		JSONParser parser = new JSONParser();
		if (object instanceof JSONObject) {
			// Already parsed
			json = (JSONObject) object;
		} else {
			try {
				json = (JSONObject) parser.parse(object.toString());
			} catch (ParseException pe) {
				return null;
			}
		}
		return convertJSONToCyData(json);
	}

	private void convertToJSON(Object obj, StringBuilder sb) {
		if (obj instanceof List) {
			List<?> list = (List)obj;
			sb.append("[");
			boolean first = true;
			for (Object o: list) {
				if (!first) sb.append(",");
				else first = false;
				convertToJSON(o, sb);
			}
			sb.append("]\n");
		} else if (obj instanceof Map) {
			sb.append("{");
			Map<?, ?> map = (Map)obj;
			boolean first = true;
			for (Object key: map.keySet()) {
				if (!first) sb.append(",");
				else first = false;
				sb.append(quote(key.toString())+": ");
				convertToJSON(map.get(key), sb);
			}
			sb.append("}\n");
		} else if (obj instanceof Number) {
			sb.append(obj.toString());
		} else if (obj instanceof Boolean) {
			sb.append(obj.toString());
		} else {
			sb.append(quote(obj.toString()));
		}
	}

	private String quote(String v) {
		return '"'+v+'"';
	}

	private CyJobData convertJSONToCyData(JSONObject json) {
		CyJobData data = getDataInstance();
		for (Object key: json.keySet()) {
			Object value = json.get(key);
			data.put(key.toString(), value);
		}
		return data;
	}

	private void addCyIdentifiable(Map<String, Object> objMap, CyNetwork net, CyIdentifiable id) {
		String name = net.getRow(id).get(CyRootNetwork.SHARED_NAME, String.class);
		objMap.put("name", name);
		objMap.put("id", id.getSUID());
	}

	private Map<String, Object> makeNodeMap(CyNetwork network, CyNode node, List<String> nodeColumns) {
		Map<String, Object> nodeMap = new HashMap<>();
		addCyIdentifiable(nodeMap, network, node);
		addColumns(nodeMap, network.getRow(node), nodeColumns);
		return nodeMap;
	}

	private Map<String, Object> makeEdgeMap(CyNetwork network, CyEdge edge, List<String> edgeColumns) {
		Map<String, Object> edgeMap = new HashMap<>();
		addCyIdentifiable(edgeMap, network, edge);
		edgeMap.put("source", getNodePointer(network, edge.getSource()));
		edgeMap.put("target", getNodePointer(network, edge.getTarget()));
		addColumns(edgeMap, network.getRow(edge), edgeColumns);
		return edgeMap;
	}

	private Map<String, Object> getNodePointer(CyNetwork network, CyNode node) {
		Map<String, Object> pointerMap = new HashMap<>();
		addCyIdentifiable(pointerMap, network, node);
		return pointerMap;
	}

	private void addColumns(Map<String, Object> map, CyRow row, List<String> columns) {
		if (columns == null || columns.size() == 0)
			return;
		for (String column: columns) {
			Object raw = row.getRaw(column);
			if (raw != null)
				map.put(column, raw);
		}
	}

	private CyNetwork findNetwork(Long suid, String name) {
		if (networkManager.networkExists(suid)) {
			return networkManager.getNetwork(suid);
		}

		CyNetwork nameNetwork = null;
		for (CyNetwork net: networkManager.getNetworkSet()) {
			String netName = net.getRow(net).get(CyRootNetwork.SHARED_NAME, String.class);
			if (netName.equals(name)) {
				if (nameNetwork == null)
					nameNetwork = net;
				else {
					// Duplicate names -- don't know what to do, so try a merge
					nameNetwork = null;
					break;
				}
			}
		}
		if (nameNetwork == null) {
			nameNetwork = networkFactory.createNetwork();
			nameNetwork.getRow(nameNetwork).set(CyRootNetwork.SHARED_NAME, name);
			nameNetwork.getRow(nameNetwork).set(CyNetwork.NAME, name);
		}

		return nameNetwork;
	}

	private Map<String, CyNode> getNodes(CyNetwork network, JSONArray nodeArray, Map<Long, ? extends CyIdentifiable> restoredIds) {
		Map<String, CyNode> nodeNameMap = new HashMap<>();
		for (CyNode node: network.getNodeList()) {
			String nodeName = network.getRow(node).get(CyRootNetwork.SHARED_NAME, String.class);
			nodeNameMap.put(nodeName, node);
		}
		for (Object obj: nodeArray) {
			JSONObject nodeMap = (JSONObject)obj;
			CyNode node = findNode(network, nodeNameMap, nodeMap, restoredIds);

			for (Object key: nodeMap.keySet()) {
				String column = (String) key;
				if (column.equals("id") || column.equals("name")) continue;
				Object columnObj = nodeMap.get(column);
				if (columnObj instanceof JSONArray)
					addListColumnData(network.getDefaultNodeTable(), node, column, (JSONArray)columnObj);
				else
					addColumnData(network.getDefaultNodeTable(), node, column, columnObj);
			}
		}
		return nodeNameMap;
	}

	private CyNode findNode(CyNetwork network, Map<String, CyNode> nodeNameMap, JSONObject nodeMap, 
	                        Map<Long, ? extends CyIdentifiable> restoredIds) {
		Long suid = (Long)nodeMap.get("id");
		String name = (String)nodeMap.get("name");
		CyNode node = null;
		if (restoredIds != null && restoredIds.containsKey(suid)) {
			node = (CyNode)restoredIds.get(suid);
			// Sanity check
			node = (CyNode)checkName(network, node, name);
		}

		if (node == null) {
			node = network.getNode(suid);
			// Sanity check
			node = (CyNode)checkName(network, node, name);
		}

		if (node == null) {
			// Ugh, do it the hard way
			if (nodeNameMap.containsKey(name)) {
				node = nodeNameMap.get(name);
			} else {
				node = network.addNode();
				network.getRow(node).set(CyRootNetwork.SHARED_NAME, name);
				network.getRow(node).set(CyRootNetwork.NAME, name);
				nodeNameMap.put(name, node);
			}
		}
		return node;
	}

	private void getEdges(CyNetwork network, Map<String, CyNode> nodeNameMap, JSONArray edgeArray,
	                      Map<Long, ? extends CyIdentifiable> restoredIds) {
		for (Object obj: edgeArray) {
			JSONObject edgeMap = (JSONObject)obj;
			Long suid = (Long)edgeMap.get("id");
			String name = (String)edgeMap.get("name");
			CyEdge edge = null;
			if (restoredIds != null && restoredIds.containsKey(suid)) {
				edge = (CyEdge)restoredIds.get(suid);
				// Sanity check
				edge = (CyEdge)checkName(network, edge, name);
			} else {
				edge = network.getEdge(suid);
				edge = (CyEdge)checkName(network, edge, name);
			}
			if (edge == null) {
				JSONObject source = (JSONObject)edgeMap.get("source");
				CyNode sourceNode = findNode(network, nodeNameMap, source, restoredIds);
				JSONObject target = (JSONObject)edgeMap.get("target");
				CyNode targetNode = findNode(network, nodeNameMap, target, restoredIds);
				edge = network.addEdge(sourceNode, targetNode, false);
				network.getRow(edge).set(CyRootNetwork.SHARED_NAME, name);
				network.getRow(edge).set(CyRootNetwork.NAME, name);
			}

			for (Object key: edgeMap.keySet()) {
				String column = (String) key;
				if (column.equals("id") || column.equals("name")) continue;
				Object columnObj = edgeMap.get(column);
				if (columnObj instanceof JSONArray)
					addListColumnData(network.getDefaultEdgeTable(), edge, column, (JSONArray)columnObj);
				else
					addColumnData(network.getDefaultEdgeTable(), edge, column, columnObj);
			}
		}
	}

	private CyIdentifiable checkName(CyNetwork net, CyIdentifiable cyId, String name) {
		String cyName = net.getRow(cyId).get(CyRootNetwork.SHARED_NAME, String.class);
		if (cyName.equals(name))
			return cyId;
		cyName = net.getRow(cyId).get(CyNetwork.NAME, String.class);
		if (cyName.equals(name))
			return cyId;
		return null;
	}

	private void addColumnData(CyTable table, CyIdentifiable cyId, String column, Object obj) {
		Class<?> type;
		Class<?> objType = getObjectClass(obj);

		if (table.getColumn(column) != null) {
			type = table.getColumn(column).getType();
		} else {
			type = objType;
			table.createColumn(column, objType, false);
		}

		table.getRow(cyId.getSUID()).set(column, obj);

	}

	private void addListColumnData(CyTable table, CyIdentifiable cyId, String column, JSONArray obj) {
		Class<?> elementType;
		Class<?> objType;
		if (table.getColumn(column) != null) {
			elementType = table.getColumn(column).getListElementType();
			if (elementType == null) return;
		} else {
			// First, see if we can even figure this out
			if (obj == null || obj.size() == 0) return;
			objType = getObjectClass(obj.get(0));
			table.createListColumn(column, objType, false);
		}

		List<Object> list = new ArrayList<Object>();
		for (Object o: obj) {
			list.add(o);
		}

		table.getRow(cyId.getSUID()).set(column, list);
	}


	private Class<?> getObjectClass(Object obj) {
		Class<?> objType;
		if (obj instanceof String)
			objType = String.class;
		else if (obj instanceof Integer)
			objType = Integer.class;
		else if (obj instanceof Long)
			objType = Long.class;
		else if (obj instanceof Double)
			objType = Double.class;
		else if (obj instanceof Boolean)
			objType = Boolean.class;
		else {
			System.out.println("addColumnData - unknown type: "+obj.getClass());
			objType = String.class;
		}
		return objType;
	}

}
