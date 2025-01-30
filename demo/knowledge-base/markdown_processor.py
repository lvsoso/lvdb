import markdown
from bs4 import BeautifulSoup
from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained('BAAI/bge-large-zh-v1.5')
model = AutoModel.from_pretrained('BAAI/bge-large-zh-v1.5')
model.eval()


def markdown_to_html(markdown_text):
    return markdown.markdown(markdown_text)

def split_html_into_segments(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    segments = []
    for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol']):
        segments.append(tag.get_text())
    return segments

def vectorize_segments(segments):
    encoded_input = tokenizer(segments, padding=True, truncation=True, return_tensors='pt')
    with torch.no_grad():
        model_output = model(**encoded_input)
        sentence_embeddings = model_output[0][:, 0]
    sentence_embeddings = torch.nn.functional.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings
