import torch
import torchvision.transforms as transforms
from torchvision.models import resnet50
from PIL import Image
import numpy as np


device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = resnet50(weights='IMAGENET1K_V1')
model = model.to(device)
model.eval()


def extract_features(image_path):

    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

    img = Image.open(image_path).convert('RGB')  # Ensure RGB format
    img_t = preprocess(img)
    batch_t = torch.unsqueeze(img_t, 0).to(device)

    with torch.no_grad():
        out = model(batch_t)

    return out.cpu().flatten().numpy()
