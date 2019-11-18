from .serializers import PostSerializer
from .models import Post
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status
import csv
import os
import boto3
from pandas import DataFrame, read_csv, ExcelWriter
import pandas as pd
import xlwt
import re
# Create your views here.

class PostView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def get(self, request, *args, **kwargs):
        posts = Post.objects.all()
        serializer = PostSerializer(posts, many=True)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        posts_serializer = PostSerializer(data=request.data)
        QueryDict = request.data
        object_dict = QueryDict.dict()
        file_name = str(object_dict['file'])
        file_realname = file_name.replace(" ", "_")
        file_realnames = file_realname.replace("̀", "")
        if posts_serializer.is_valid():
            posts_serializer.save()

            #Nettoie afin qu'il soit facilement facilement lisible et iterable
            df = pd.read_excel('./file/post_images/' + file_realnames, sheet_name='BAREME-MEASUREMENT CHART', header=1)
            df = df.drop(df.columns[[2]], axis=1)
            df = df.iloc[:, :-8]
            match = re.search(r'1002[45678]\d{2}', file_name)

            #Condition utilisé pour récolter les lignes qui nous intéressent ( regex re.search())
            if match:
                searchfor = ['Hauteur taille - hanche', 'Tour de taille\n1/2 Waist round', 'Tour de bassin\n1/2 Hips round',
                 'Enfourchement dos avec ceinture\n', 'dos avec ceinture' 'cuisse', 'Enfourchement',
                 'Longueur d\'entrejambe', 'Tour de mollet\n', 'Longueur de jambe ( taille-terre)\n']

            #Seconde façon de faire non utilisé dans le script ( if x in :)
            elif '1002827' in file_name or '1002781' in file_name:
                print('est la aussi')
                searchfor = ['Hauteur taille - hanche', 'Tour de taille\n1/2 Waist round', 'Tour de bassin\n1/2 Hips round',
                 'Enfourchement dos avec ceinture\n', 'dos avec ceinture' 'cuisse', 'Enfourchement',
                 'Longueur d\'entrejambe', 'Tour de mollet\n', 'Longueur de jambe ( taille-terre)\n']

            # Enregistre seulement les lignes recherchées et écrit le tout dans un nouveau fichier xls
            df = df[df['TAILLES FRANCAISES '].str.contains('|'.join(searchfor), na=False) ]
            writer = ExcelWriter('export' + file_realname)
            df.to_excel(writer)
            writer.save()

            #Upload to S3
            s3 = boto3.client('s3')
            bucket_name = 'compartiment-thimothe'
            s3.upload_file('export' + str(file_realname), bucket_name, 'export' + str(file_realname))
            print('Successfully uploaded')



            return Response(posts_serializer.data, status=status.HTTP_201_CREATED)
        else:
            print('error', posts_serializer.errors)
            return Response(posts_serializer.errors, status=status.HTTP_400_BAD_REQUEST)