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

import de.hu_berlin.german.korpling.saltnpepper.pepper.common.DOCUMENT_STATUS;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.MappingSubject;
import de.hu_berlin.german.korpling.saltnpepper.pepper.modules.PepperMapper;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sCorpusStructure.SDocument;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sDocumentStructure.SDocumentGraph;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sDocumentStructure.STextualDS;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCommon.sDocumentStructure.STextualRelation;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SElementId;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SNode;
import de.hu_berlin.german.korpling.saltnpepper.salt.saltCore.SRelation;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.eclipse.emf.common.util.EList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Thomas Krause <krauseto@hu-berlin.de>
 */
public class AppendMapper extends BaseMapper implements PepperMapper {

	private final static Logger logger = LoggerFactory.getLogger(
		AppendMapper.class);

	@Override
	public DOCUMENT_STATUS mapSDocument() {
		
		// only do something if there are at least two documents that need to be merged
		if (getMappingSubjects().size() > 1) {
			MappingSubject baseSubject = getMappingSubjects().get(0);
			SDocument baseDoc = (SDocument) baseSubject.getSElementId().
				getSIdentifiableElement();
			SDocumentGraph baseGraph = baseDoc.getSDocumentGraph();
			STextualDS baseText = null;

			Map<SElementId, STextualDS> doc2text = new LinkedHashMap<>();

			StringBuilder appendedText = new StringBuilder();
			ListIterator<MappingSubject> itSubjects = getMappingSubjects().
				listIterator();
			while (itSubjects.hasNext()) {
				MappingSubject s = itSubjects.next();
				SDocument doc = (SDocument) s.getSElementId().getSIdentifiableElement();
				EList<STextualDS> textList = doc.getSDocumentGraph().getSTextualDSs();
				if (textList != null && !textList.isEmpty()) {
					STextualDS firstText = textList.get(0);

					if (baseText == null) {
						baseText = firstText;
					}

					doc2text.put(s.getSElementId(), firstText);

					int offset = appendedText.length();

					if (textList.size() > 1) {
						logger.error("Merging two documents by appending them "
							+ "only works if the documents have only one text, "
							+ "but document {} has {} texts! Only the first one will be merged.",
							doc.getSId(), textList.size());
					}
					appendedText.append(firstText.getSText());
					if (itSubjects.hasNext()) {
						appendedText.append("\n");
					}

					if (itSubjects.hasPrevious()) {
						// copy all nodes from the other document graphs to the first one

						List<SRelation> originalRelations
							= new LinkedList<>(doc.getSDocumentGraph().
								getSRelations());
						
						for (SNode n : new LinkedList<>(doc.getSDocumentGraph().getSNodes())) {
							if (!(n instanceof STextualDS)) {
								doc.getSDocumentGraph().removeNode(n);
								baseGraph.addSNode(n);
							}
						}

						for (SRelation rel : originalRelations) {
							if (rel instanceof STextualRelation) {
								STextualRelation textRel = (STextualRelation) rel;
								// only move the first textual relation
								if (textRel.getSTextualDS() == firstText) {
									
									// reset the actual text and the start and end
									textRel.setSTextualDS(baseText);
									textRel.setSStart(offset + textRel.getSStart());
									textRel.setSEnd(offset + textRel.getSEnd());
									
									baseGraph.addSRelation(textRel);
									
								}
							} else {
								// move all other relations
								baseGraph.addSRelation(rel);
							}
						}
					}
				}
			}

			// set the new text
			if (baseText != null) {
				baseText.setSText(appendedText.toString());
			}
			
			// delete all other documents
			ListIterator<MappingSubject> itRemaining = getMappingSubjects().
				listIterator(1);
			while (itRemaining.hasNext()) {
				MappingSubject s = itRemaining.next();
				s.setMappingResult(DOCUMENT_STATUS.DELETED);
			}
		}

		return DOCUMENT_STATUS.COMPLETED;
	}
}
