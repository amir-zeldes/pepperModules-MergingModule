/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.hu_berlin.german.korpling.saltnpepper.pepperModules.mergingModules;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperManipulator;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.exceptions.PepperModuleException;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpus;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SCorpusGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Thomas Krause <krauseto@hu-berlin.de>
 */
@Component(name = "AppenderComponent", factory
	= "PepperManipulatorComponentFactory")
public class Appender extends BaseManipulator implements PepperManipulator {

	private final static Logger logger = LoggerFactory.getLogger(Appender.class);

	private final Map<SDocument, String> targetDocRename = new LinkedHashMap<>();
	
	private final Map<SElementId, SElementId> source2TargetDoc = new HashMap<>();

	public Appender() {
		setName("Appender");
	}

	@Override
	public List<SElementId> proposeImportOrder(SCorpusGraph sCorpusGraph) {

		setBaseCorpusStructure(sCorpusGraph);
		List<SElementId> result = null;

		if (sCorpusGraph != null) {

			ImmutableListMultimap<String, SDocument> documentsByName
				= Multimaps.index(sCorpusGraph.getSDocuments(), new Function<SDocument, String>() {
					@Override
					public String apply(SDocument input) {
						return input.getSName();
					}
				});

			// All documents are merged into the first document of the list, but after that we want
			// to rename this first document to the name given by the user. 
			targetDocRename.clear();

			Set<SElementId> manualDefinedDocs = new HashSet<>();

			// the imports must be in the same order as the manually defined lists of documents
			// (the first document in the list must be first)
			result = new ArrayList<>();

			mappingTable = new SNodeByIDStorage();
			source2TargetDoc.clear();

			// the toplevel corpus and sub-corpora are always mapped to itself
			for (SCorpus c : sCorpusGraph.getSCorpora()) {
				mappingTable.put(c.getSId(), c);
			}

			// iterate over all properties to get manually defined document name mappings
			if (getProperties() != null) {

				for (String propName : getProperties().getPropertyNames()) {
					if (propName.startsWith(MergerProperties.PROP_DOC_MAPPING_PREFIX)) {
						String targetDocName = propName.substring(MergerProperties.PROP_DOC_MAPPING_PREFIX.length());
						String sourceDocsRaw = (String) getProperties().getProperty(propName).getValue();

						SDocument targDocument = null;

						for (String sourceDocName : Splitter.on(",").trimResults()
							.omitEmptyStrings().split(sourceDocsRaw)) {

							ImmutableList<SDocument> sourceDocList = documentsByName.get(sourceDocName);
							if (sourceDocList.size() > 1) {
								logger.warn("There is more than one document "
									+ "with the name {} in the corpus, the "
									+ "appender will only consider the first "
									+ "one and deletes all others.", sourceDocName);
							} else if (sourceDocList.isEmpty()) {
								logger.warn("No document found with name {}, "
									+ "the configuration will be ignored", sourceDocName);
								continue;
							}

							SDocument sourceDoc = sourceDocList.get(0);

							if (targDocument == null) {
								// the first document in the list is always the target document
								targDocument = sourceDoc;
								targetDocRename.put(sourceDoc, targetDocName);
							}
							mappingTable.put(targDocument.getSId(), sourceDoc);
							source2TargetDoc.put(sourceDoc.getSElementId(), targDocument.getSElementId());
							manualDefinedDocs.add(sourceDoc.getSElementId());

							result.add(sourceDoc.getSElementId());
						}
					}
				}
			}
			// add all non-manually defined documents as well
			for (SDocument doc : sCorpusGraph.getSDocuments()) {
				if (!manualDefinedDocs.contains(doc.getSElementId())) {
					mappingTable.put(doc.getSId(), doc);
				}
			}

		}

		return result;
	}

	@Override
	public void end() throws PepperModuleException {
		// apply re-naming
		for(Map.Entry<SDocument, String> e : targetDocRename.entrySet()) {
			e.getKey().setSName(e.getValue());
		}
		super.end();
	}
	
	

	@Override
	protected void enhanceBaseCorpusStructure() {
	}

	@Override
	public BaseMapper newMapperInstance() {
		return new AppendMapper();
	}

	@Override
	protected List<SNode> getMappableSlot(SElementId sElementId) {
		SElementId targetID = source2TargetDoc.get(sElementId);
		if(targetID == null) {
			// fallback to the mapping table
			return mappingTable.get(sElementId.getSId());
		} else {
			// return the list of documents to merge associated with the actual target ID
			return mappingTable.get(targetID.getSId());
		}
		
	}
	
	

	@Override
	protected List<SElementId> getOrCreateGivenSlot(SElementId sElementId) {
		SElementId targetID = source2TargetDoc.get(sElementId);
		
		
		if(targetID == null) {
			// fallback
			targetID = sElementId;
		} 
		
		List<SElementId> givenSlot = givenSlots.get(targetID.getSId());
		if (givenSlot == null) {
			givenSlot = new Vector<>();
			givenSlots.put(targetID.getSId(), givenSlot);
		}
		return givenSlot;
	}

}
