/**
 * Copyright 2009 Humboldt University of Berlin, INRIA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.osgi.service.component.annotations.Component;

import de.hu_berlin.german.korpling.saltnpepper.pepper.exceptions.PepperFWException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.DocumentController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperManipulator;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapperController;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.exceptions.PepperModuleException;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.impl.PepperManipulatorImpl;
import de.hu_berlin.german.korpling.saltnpepper.salt.SaltFactory;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotatableElement;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SMetaAnnotation;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;

/**
 * 
 * @author Florian Zipser
 * @version 1.0
 *
 */
@Component(name="MergerComponent", factory="PepperManipulatorComponentFactory")
public class Merger extends PepperManipulatorImpl implements PepperManipulator
{
	/**
	 * Determins the merging level concerning the corpus-structure of how a corpus should be merged.
	 * @author florian
	 *
	 */
	public enum MERGING_LEVEL{
		/** just copies all corpus graphs to a single one **/
		MERGE_CORPUS_GRAPHS,
		/** merges all corpus graphs into a single corpus graph, but does not merge the documents **/
		MERGE_DOCUMENT_PATHS,
		/** merges all possible documents (by given table or autodetect) **/
		MERGE_DOCUMENTS
	}
	
	public Merger()
	{
		super();
		setName("Merger");
		setProperties(new MergerProperties());
	}
	/** 
	 * A table containing the import order for {@link SElementId} corresponding to {@link SDocument}
	 * and {@link SCorpus} nodes corresponding to the {@link SCorpusGraph} they are contained in.
	 **/
	private Map<SCorpusGraph, List<SElementId>> importOrder= null;
	
	/** a map containing all mapping partners ({@link SCorpus} and {@link SDocument} nodes) corresponding to their sId. **/
	protected Multimap mappingTable= null;
	
	/** similar to guavas multimap, but can contain values twice (this is because, equal method of two {@link SDocument}s having the same path but belong to different {@link SCorpusGraph}s are the same for equals(), but shouldn't be.)**/ 
	class Multimap{
		private Map<String, List<SNode>> map= null;
		
		public Multimap(){
			map= new HashMap<String, List<SNode>>();
		}
		
		public void put(String sId, SNode sNode){
			List<SNode> slot= map.get(sId);
			if (slot== null){
				slot= new ArrayList<SNode>();
				map.put(sId, slot);
			}
			slot.add(sNode);
		}
		public List<SNode> get(String sId){
			return(map.get(sId));
		}
		@Override
		public String toString(){
			StringBuilder retVal= new StringBuilder();
			for (String key: map.keySet()){
				retVal.append(key);
				retVal.append("=");
				retVal.append(map.get(key));
			}
			return(retVal.toString());
		}
		public Set<String> keySet(){
			return(map.keySet());
		}
	}
	
	/**
	 * Emits a base graph.
	 */
	protected synchronized void createMapping(){
		if (mappingTable== null){
			//initialize importOrder
			importOrder= new HashMap<SCorpusGraph, List<SElementId>>();
			for (SCorpusGraph graph: getSaltProject().getSCorpusGraphs()){
				importOrder.put(graph, new ArrayList<SElementId>());
			}
			
			mappingTable= new Multimap();
			//TODO add mapping properties to table
			for (SCorpusGraph graph: getSaltProject().getSCorpusGraphs()){
				if (graph.getSCorpora().size()!= 0){
					for (SCorpus sCorpus: graph.getSCorpora()){
						//TODO check if sCorpus.getSId() is contained in mapping properties
						mappingTable.put(sCorpus.getSId(), sCorpus);
					}
					for (SDocument sDocument: graph.getSDocuments()){
						
						//TODO check if sDocument.getSId() is contained in mapping properties 
						mappingTable.put(sDocument.getSId(), sDocument);
					}
				}
			}
			List<List<List<SNode>>> listOfLists= new ArrayList<List<List<SNode>>>(getSaltProject().getSCorpusGraphs().size());
			for (int i= 0; i < getSaltProject().getSCorpusGraphs().size(); i++){
				listOfLists.add(new ArrayList<List<SNode>>());
			}
			
			for (String key: mappingTable.keySet()){
				List<SNode> nodes= mappingTable.get(key);
				listOfLists.get(nodes.size()-1).add(nodes);
			}
			
			for (int i= getSaltProject().getSCorpusGraphs().size(); i>0;i--){
				List<List<SNode>> list= listOfLists.get(i-1);
				for (List<SNode> nodes: list){
					for (SNode node: nodes){
						if (node instanceof SDocument){
							importOrder.get(((SDocument)node).getSCorpusGraph()).add(node.getSElementId());
						}
					}
				}
			}
		}
	}
	/**
	 * Moves all {@link SMetaAnnotation}s from <em>source</em> object to passed <em>target</em> object. 
	 * @param source
	 * @param target
	 */
	protected static void moveSMetaAnnotations(SMetaAnnotatableElement source, SMetaAnnotatableElement target){
		if (	(source!= null)&&
				(target!= null)){
			SMetaAnnotation sMeta= null;
			int offset= 0;
			while(source.getSMetaAnnotations().size()>offset){
				sMeta= source.getSMetaAnnotations().get(0);
				if (target.getSMetaAnnotation(SaltFactory.eINSTANCE.createQName(sMeta.getSNS(), sMeta.getSName()))== null){
					target.addSMetaAnnotation(sMeta);
				}else offset++;
			}
		}
	}
	/**
	 * Creates an import order for each {@link SCorpusGraph} object. The order for given {@link SCorpusGraph} objects
	 * is very similar or equal, in case they contain the same {@link SDocument}s (the ones to be merged).
	 */
	@Override
	public List<SElementId> proposeImportOrder(SCorpusGraph sCorpusGraph) {
		List<SElementId> retVal= null;
		if (sCorpusGraph!= null){
			if (getSaltProject().getSCorpusGraphs().size()>1)
			{
				if (!MERGING_LEVEL.MERGE_CORPUS_GRAPHS.equals(((MergerProperties)getProperties()).getMergingLevel())){
					createMapping();
					if (importOrder!= null){
						retVal= importOrder.get(sCorpusGraph);
					}
				}
			}
		}
		return(retVal);
	}
	/** This table stores all corresponding mergable {@link SElementId}.*/
	private Map<String, List<SElementId>> givenSlots= null; 
	
	/**
	 * {@inheritDoc PepperModule#start()}
	 * Overrides parent method, to enable the parallel working in more than one {@link DocumentController} objects
	 * at a time.
	 */
	@Override
	public void start() throws PepperModuleException
	{
		if (getSaltProject()== null)
			throw new PepperFWException("No salt project was set in module '"+getName()+", "+getVersion()+"'.");
		//creating new thread group for mapper threads
		setMapperThreadGroup(new ThreadGroup(Thread.currentThread().getThreadGroup(), this.getName()+"_mapperGroup"));
		givenSlots= new Hashtable<String, List<SElementId>>();
		boolean isStart= true;
		SElementId sElementId= null;
		DocumentController documentController= null;
		while ((isStart) || (sElementId!= null))
		{	
			isStart= false;
			documentController= this.getModuleController().next();
			if (documentController== null){
				break;
			}
			sElementId= documentController.getsDocumentId();
			getDocumentId2DC().put(SaltFactory.eINSTANCE.getGlobalId(sElementId), documentController);
			
			
			List<SNode> mappableSlot= mappingTable.get(sElementId.getSId());
			List<SElementId> givenSlot= givenSlots.get(sElementId.getSId());
			if (givenSlot== null){
				givenSlot= new Vector<SElementId>();
				givenSlots.put(sElementId.getSId(), givenSlot);
			}
			givenSlot.add(sElementId);
			if (givenSlot.size() < mappableSlot.size()){
				documentController.sendToSleep();
			}else if (givenSlot.size()== mappableSlot.size()){
				try{
					start(sElementId);
				}catch (Exception e){
					throw new PepperModuleException("",e);
				}
			}else throw new PepperModuleException(this, "This should not have beeen happend and is a bug of module. The problem is, 'givenSlot.size()' is higher than 'mappableSlot.size()'.");
		}	
		Collection<PepperMapperController> controllers=null;
		HashSet<PepperMapperController> alreadyWaitedFor= new HashSet<PepperMapperController>();
		//wait for all SDocuments to be finished
		controllers= Collections.synchronizedCollection(this.getMapperControllers().values());
		for (PepperMapperController controller: controllers)
		{
			try {
				controller.join();
				alreadyWaitedFor.add(controller);
			} catch (InterruptedException e) {
				throw new PepperFWException("Cannot wait for mapper thread '"+controller+"' in "+this.getName()+" to end. ", e);
			}
			this.done(controller);
		}
		
		this.end();
		//only wait for  controllers which have been added by end()
		for (PepperMapperController controller: this.getMapperControllers().values())
		{
			if (!alreadyWaitedFor.contains(controller)){
				try {
					controller.join();
				} catch (InterruptedException e) {
					throw new PepperFWException("Cannot wait for mapper thread '"+controller+"' in "+this.getName()+" to end. ", e);
				}
				this.done(controller);
			}
		}
	}
	/** 
	 * Creates a {@link PepperMapper} of type {@link MergerMapper}. Therefore the table {@link #givenSlots} 
	 * must contain an entry for the given {@link SElementId}. The create methods passes all documents 
	 * and corpora given in the entire slot to the {@link MergerMapper}. 
	 **/
	@Override
	public PepperMapper createPepperMapper(SElementId sElementId) {
		MergerMapper mapper= new MergerMapper();
		
		return(mapper);
	}
}	